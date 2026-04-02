"""
Freemail 邮箱服务实现
基于自部署 Cloudflare Worker 临时邮箱服务 (https://github.com/idinging/freemail)
"""

import re
import time
import logging
import random
import string
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
from pathlib import Path

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..core.http_client import HTTPClient, RequestConfig
from ..config.constants import OTP_CODE_PATTERN

logger = logging.getLogger(__name__)


def _normalize_domain(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    if "://" in text:
        parsed = urlparse(text)
        candidate = parsed.hostname or parsed.netloc or parsed.path
        text = candidate or text

    return text.strip().lstrip("@").rstrip("/").lower()


def _normalize_domain_list(value: Any) -> List[str]:
    if isinstance(value, list):
        domains = [_normalize_domain(item) for item in value]
    else:
        text = str(value or "").strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                import json

                parsed = json.loads(text)
                if isinstance(parsed, list):
                    domains = [_normalize_domain(item) for item in parsed]
                else:
                    domains = [_normalize_domain(text)]
            except Exception:
                domains = [_normalize_domain(item) for item in text.split(",")]
        elif "," in text:
            domains = [_normalize_domain(item) for item in text.split(",")]
        else:
            domains = [_normalize_domain(text)]

    result: List[str] = []
    for domain in domains:
        if domain and domain not in result:
            result.append(domain)
    return result


def _to_timestamp(value: Any) -> Optional[float]:
    """将 Freemail 返回的时间字段转换为 Unix 时间戳。"""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            pass
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return None
    return None


def _looks_like_naive_datetime_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    if "T" not in text and " " not in text:
        return False
    if text.endswith("Z"):
        return False
    time_part = text.split("T", 1)[-1] if "T" in text else text.rsplit(" ", 1)[-1]
    return "+" not in time_part and "-" not in time_part


def _extract_mail_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("emails", "mails", "results", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


class FreemailService(BaseEmailService):
    """
    Freemail 邮箱服务
    基于自部署 Cloudflare Worker 的临时邮箱
    """

    _domain_health_lock = None
    _domain_create_lock = None
    _domain_rotation_offsets: Dict[str, int] = {}
    _runtime_domain_block_until: Dict[str, float] = {}

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        """
        初始化 Freemail 服务

        Args:
            config: 配置字典，支持以下键:
                - base_url: Worker 域名地址 (必需)
                - admin_token: Admin Token，对应 JWT_TOKEN (必需)
                - domain: 邮箱域名，如 example.com
                - timeout: 请求超时时间，默认 30
                - max_retries: 最大重试次数，默认 3
            name: 服务名称
        """
        super().__init__(EmailServiceType.FREEMAIL, name)

        required_keys = ["base_url", "admin_token"]
        missing_keys = [key for key in required_keys if not (config or {}).get(key)]
        if missing_keys:
            raise ValueError(f"缺少必需配置: {missing_keys}")

        default_config = {
            "timeout": 30,
            "max_retries": 3,
            "exploratory_probe_slots": 2,
        }
        self.config = {**default_config, **(config or {})}
        self.config["base_url"] = self.config["base_url"].rstrip("/")
        self.config["domain"] = _normalize_domain_list(self.config.get("domain"))
        try:
            self.config["exploratory_probe_slots"] = max(0, int(self.config.get("exploratory_probe_slots", 2)))
        except (TypeError, ValueError):
            self.config["exploratory_probe_slots"] = 2

        http_config = RequestConfig(
            timeout=self.config["timeout"],
            max_retries=self.config["max_retries"],
        )
        self.http_client = HTTPClient(proxy_url=None, config=http_config)

        if FreemailService._domain_health_lock is None:
            import threading
            FreemailService._domain_health_lock = threading.Lock()
        if FreemailService._domain_create_lock is None:
            import threading
            FreemailService._domain_create_lock = threading.Lock()

        # 缓存 domain 列表
        self._domains = []
        # 按 OTP 阶段缓存已消费邮件，避免重试时反复捡回旧邮件
        self._stage_seen_mail_ids: Dict[str, set] = {}
        # 跨阶段记录最近一次真正消费过的邮件，避免登录阶段再次捡到注册阶段旧邮件
        self._last_used_mail_ids: Dict[str, str] = {}
        self._last_runtime_metrics: Dict[str, Any] = {}

    @staticmethod
    def _health_store_path() -> Path:
        app_root = Path(__file__).resolve().parents[2]
        data_dir = app_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "freemail_domain_health.json"

    @classmethod
    def _load_domain_health(cls) -> Dict[str, Any]:
        path = cls._health_store_path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("读取 Freemail 域名健康池失败: %s", exc)
            return {}

    @classmethod
    def _save_domain_health(cls, payload: Dict[str, Any]) -> None:
        path = cls._health_store_path()
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)

    def _health_bucket_key(self) -> str:
        return str(self.config.get("base_url") or "").strip().lower()

    def _collect_domain_health_snapshot(self, requested_domain: Any = None) -> Dict[str, Any]:
        self._ensure_domains()
        configured_domains = _normalize_domain_list(
            requested_domain if requested_domain not in (None, "") else self.config.get("domain")
        )
        runtime_domains = list(self._domains or [])
        domain_list = configured_domains or runtime_domains
        if runtime_domains:
            domain_list = [domain for domain in runtime_domains if not configured_domains or domain in configured_domains]
        domain_list = domain_list or configured_domains or runtime_domains

        bucket_key = self._health_bucket_key()
        now_ts = time.time()

        with FreemailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(bucket_key, {"domains": {}})
            states = bucket.setdefault("domains", {})

            available_domains: List[str] = []
            cooldown_domains: List[Dict[str, Any]] = []
            domain_states: Dict[str, Dict[str, Any]] = {}

            for domain in domain_list:
                state = states.setdefault(domain, {})
                persisted_until = float(state.get("cooldown_until") or 0.0)
                runtime_until = float(FreemailService._runtime_domain_block_until.get(f"{bucket_key}::{domain}") or 0.0)
                cooldown_until = max(persisted_until, runtime_until)
                item = {
                    "success_count": int(state.get("success_count") or 0),
                    "fail_count": int(state.get("fail_count") or 0),
                    "consecutive_failures": int(state.get("consecutive_failures") or 0),
                    "register_create_account_retryable_count": int(
                        state.get("register_create_account_retryable_count") or 0
                    ),
                    "last_error": str(state.get("last_error") or "").strip(),
                    "last_outcome": str(state.get("last_outcome") or "").strip(),
                    "cooldown_until": cooldown_until,
                    "is_proven": int(state.get("success_count") or 0) > 0,
                    "is_cooling": cooldown_until > now_ts,
                }
                domain_states[domain] = item
                if cooldown_until > now_ts:
                    cooldown_domains.append(
                        {
                            "domain": domain,
                            "cooldown_until": cooldown_until,
                            "cooldown_until_iso": datetime.fromtimestamp(cooldown_until).isoformat(),
                            "remaining_seconds": max(0, int(cooldown_until - now_ts)),
                            "last_error": item["last_error"],
                            "last_outcome": item["last_outcome"],
                        }
                    )
                else:
                    available_domains.append(domain)

            self._save_domain_health(payload)

        cooldown_domains.sort(key=lambda item: (item.get("cooldown_until") or 0.0, item.get("domain") or ""))
        return {
            "service_type": self.service_type.value,
            "base_url": bucket_key,
            "configured_domains": domain_list,
            "available_domains": available_domains,
            "cooldown_domains": cooldown_domains,
            "has_available_domains": bool(available_domains),
            "domain_states": domain_states,
        }

    @staticmethod
    def _domain_priority_key(state: Dict[str, Any]) -> tuple:
        success_count = int(state.get("success_count") or 0)
        consecutive_failures = int(state.get("consecutive_failures") or 0)
        fail_count = int(state.get("fail_count") or 0)
        retryable_count = int(state.get("register_create_account_retryable_count") or 0)
        updated_at = str(state.get("updated_at") or "")
        is_proven = 1 if success_count > 0 else 0
        return (
            -is_proven,
            -success_count,
            consecutive_failures,
            retryable_count,
            fail_count,
            updated_at,
        )

    def _get_candidate_domains(self, requested_domain: Any = None) -> List[str]:
        snapshot = self._collect_domain_health_snapshot(requested_domain)
        configured_domains = list(snapshot.get("configured_domains") or [])
        if not configured_domains:
            return []
        healthy_domains = list(snapshot.get("available_domains") or [])
        if not healthy_domains:
            return []

        bucket_key = self._health_bucket_key()
        with FreemailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(bucket_key, {"domains": {}})
            states = bucket.setdefault("domains", {})
            ordered = sorted(
                healthy_domains,
                key=lambda domain: self._domain_priority_key(states.get(domain, {})),
            )
            self._save_domain_health(payload)

        proven_domains = [d for d in ordered if int(states.get(d, {}).get("success_count") or 0) > 0]
        exploratory_domains = [d for d in ordered if d not in proven_domains]

        if proven_domains:
            if exploratory_domains:
                rotation_key = f"{bucket_key}::{','.join(exploratory_domains)}"
                next_offset = FreemailService._domain_rotation_offsets.get(rotation_key, 0)
                probe_slots = max(0, int(self.config.get("exploratory_probe_slots") or 0))
                if probe_slots <= 0:
                    return proven_domains
                rotated = exploratory_domains[next_offset:] + exploratory_domains[:next_offset]
                probes = rotated[:probe_slots]
                FreemailService._domain_rotation_offsets[rotation_key] = (next_offset + 1) % len(exploratory_domains)
                return proven_domains + probes
            return proven_domains

        rotation_key = f"{bucket_key}::{','.join(ordered)}"
        next_offset = FreemailService._domain_rotation_offsets.get(rotation_key, 0)
        rotated = ordered[next_offset:] + ordered[:next_offset]
        FreemailService._domain_rotation_offsets[rotation_key] = (next_offset + 1) % len(ordered)
        return rotated

    def _cooldown_domain(self, domain: str, *, outcome: str, cooldown_seconds: int, error_message: str) -> None:
        bucket_key = self._health_bucket_key()
        now_ts = time.time()
        now_iso = datetime.now().isoformat()
        with FreemailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(bucket_key, {"domains": {}})
            state = bucket.setdefault("domains", {}).setdefault(domain, {})
            state["updated_at"] = now_iso
            state["last_error"] = str(error_message or "").strip()
            state["last_outcome"] = outcome
            state["fail_count"] = int(state.get("fail_count") or 0) + 1
            state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1
            state["cooldown_until"] = max(float(state.get("cooldown_until") or 0.0), now_ts + max(0, cooldown_seconds))
            FreemailService._runtime_domain_block_until[f"{bucket_key}::{domain}"] = state["cooldown_until"]
            self._save_domain_health(payload)
        logger.warning("Freemail 域名进入冷却: %s, outcome=%s, cooldown=%ss", domain, outcome, cooldown_seconds)

    @staticmethod
    def _classify_domain_error(error_message: str) -> str:
        text = str(error_message or "").strip().lower()
        if not text:
            return "success"
        if "registration_disallowed" in text or "cannot create your account with the given information" in text:
            return "domain_blocked"
        if "failed to create account. please try again." in text:
            return "register_create_account_retryable"
        if "等待验证码超时" in text or "验证验证码失败" in text:
            return "otp_timeout"
        if "创建邮箱失败" in text:
            return "mailbox_create_failed"
        if "http 503" in text or "http 429" in text:
            return "upstream_transient"
        return "other"

    def report_registration_outcome(
        self,
        email: str,
        *,
        success: bool,
        error_message: str = "",
    ) -> None:
        email_text = str(email or "").strip().lower()
        if "@" not in email_text:
            return
        domain = email_text.split("@", 1)[1].strip()
        if not domain:
            return

        bucket_key = self._health_bucket_key()
        outcome = self._classify_domain_error(error_message)
        now_ts = time.time()
        now_iso = datetime.now().isoformat()

        with FreemailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(bucket_key, {"domains": {}})
            state = bucket.setdefault("domains", {}).setdefault(domain, {})
            state["updated_at"] = now_iso
            state["last_error"] = str(error_message or "").strip()
            state["last_outcome"] = outcome

            if success:
                state["success_count"] = int(state.get("success_count") or 0) + 1
                state["consecutive_failures"] = 0
                state["register_create_account_retryable_count"] = 0
                state["cooldown_until"] = 0
                FreemailService._runtime_domain_block_until.pop(f"{bucket_key}::{domain}", None)
                self._save_domain_health(payload)
                return

            if outcome == "upstream_transient":
                self._save_domain_health(payload)
                return

            state["fail_count"] = int(state.get("fail_count") or 0) + 1
            state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1

            cooldown_seconds = 0
            if outcome == "domain_blocked":
                cooldown_seconds = 12 * 3600
            elif outcome == "otp_timeout":
                if int(state.get("consecutive_failures") or 0) >= 3:
                    cooldown_seconds = 30 * 60
            elif outcome == "mailbox_create_failed":
                if int(state.get("consecutive_failures") or 0) >= 3:
                    cooldown_seconds = 15 * 60
            elif outcome == "register_create_account_retryable":
                state["register_create_account_retryable_count"] = int(
                    state.get("register_create_account_retryable_count") or 0
                ) + 1
                success_count = int(state.get("success_count") or 0)
                consecutive_failures = int(state.get("consecutive_failures") or 0)
                retryable_count = int(state.get("register_create_account_retryable_count") or 0)
                if success_count <= 0:
                    if consecutive_failures >= 2:
                        cooldown_seconds = 30 * 60
                    if retryable_count >= 4:
                        cooldown_seconds = max(cooldown_seconds, 12 * 3600)
                else:
                    if consecutive_failures >= 3:
                        cooldown_seconds = 15 * 60
                    elif consecutive_failures >= 2:
                        cooldown_seconds = 5 * 60

            if cooldown_seconds > 0:
                state["cooldown_until"] = max(float(state.get("cooldown_until") or 0.0), now_ts + cooldown_seconds)
                FreemailService._runtime_domain_block_until[f"{bucket_key}::{domain}"] = state["cooldown_until"]
            self._save_domain_health(payload)

    def get_domain_health_snapshot(self) -> Dict[str, Any]:
        return self._collect_domain_health_snapshot()

    def get_runtime_metrics(self) -> Dict[str, Any]:
        return dict(self._last_runtime_metrics or {})

    def _get_headers(self) -> Dict[str, str]:
        """构造 admin 请求头"""
        return {
            "Authorization": f"Bearer {self.config['admin_token']}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _make_request(self, method: str, path: str, **kwargs) -> Any:
        """
        发送请求并返回 JSON 数据

        Args:
            method: HTTP 方法
            path: 请求路径（以 / 开头）
            **kwargs: 传递给 http_client.request 的额外参数

        Returns:
            响应 JSON 数据

        Raises:
            EmailServiceError: 请求失败
        """
        url = f"{self.config['base_url']}{path}"
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self._get_headers())

        try:
            response = self.http_client.request(method, url, **kwargs)

            if response.status_code >= 400:
                error_msg = f"请求失败: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = f"{error_msg} - {error_data}"
                except Exception:
                    error_msg = f"{error_msg} - {response.text[:200]}"
                self.update_status(False, EmailServiceError(error_msg))
                raise EmailServiceError(error_msg)

            try:
                return response.json()
            except Exception:
                return {"raw_response": response.text}

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"请求失败: {method} {path} - {e}")

    def _ensure_domains(self):
        """获取并缓存可用域名列表"""
        if not self._domains:
            try:
                domains = self._make_request("GET", "/api/domains")
                if isinstance(domains, list):
                    self._domains = _normalize_domain_list(domains)
            except Exception as e:
                logger.warning(f"获取 Freemail 域名列表失败: {e}")

    def _resolve_domain_index(self, requested_domain: Any = None) -> int:
        self._ensure_domains()

        if not self._domains:
            return 0

        configured_domains = _normalize_domain_list(
            requested_domain if requested_domain not in (None, "") else self.config.get("domain")
        )

        if not configured_domains:
            return random.randrange(len(self._domains))

        matched = [domain for domain in self._domains if domain in configured_domains]
        if not matched:
            matched = self._domains

        rotation_key = ",".join(matched)
        next_offset = FreemailService._domain_rotation_offsets.get(rotation_key, 0)
        target_domain = matched[next_offset % len(matched)]
        FreemailService._domain_rotation_offsets[rotation_key] = (next_offset + 1) % len(matched)
        return self._domains.index(target_domain)

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        通过 API 创建临时邮箱

        Returns:
            包含邮箱信息的字典:
            - email: 邮箱地址
            - service_id: 同 email（用作标识）
        """
        req_config = config or {}
        prefix = req_config.get("name")
        specified_domain = req_config.get("domain")

        with FreemailService._domain_create_lock:
            self._ensure_domains()
            candidate_domains = self._get_candidate_domains(specified_domain)
            if not candidate_domains:
                snapshot = self._collect_domain_health_snapshot(specified_domain)
                cooldown_domains = list(snapshot.get("cooldown_domains") or [])
                if cooldown_domains:
                    cooldown_summary = ", ".join(
                        f"{item['domain']}({item['remaining_seconds']}s,{item['last_outcome'] or 'cooling'})"
                        for item in cooldown_domains
                    )
                    raise EmailServiceError(
                        "当前无可用邮箱域名，以下域名正在冷却中不可用: "
                        f"{cooldown_summary}"
                    )
                raise EmailServiceError("当前无可用邮箱域名")

            last_error: Optional[Exception] = None
            for idx, selected_domain in enumerate(candidate_domains, start=1):
                try:
                    if self._domains and selected_domain in self._domains:
                        domain_index = self._domains.index(selected_domain)
                    else:
                        domain_index = self._resolve_domain_index(selected_domain)

                    if prefix:
                        body = {
                            "local": prefix,
                            "domainIndex": domain_index
                        }
                        resp = self._make_request("POST", "/api/create", json=body)
                    else:
                        params = {"domainIndex": domain_index}
                        length = req_config.get("length")
                        if length:
                            params["length"] = length
                        resp = self._make_request("GET", "/api/generate", params=params)

                    email = resp.get("email")
                    if not email:
                        raise EmailServiceError(f"创建邮箱失败，未返回邮箱地址: {resp}")

                    email_info = {
                        "email": email,
                        "service_id": email,
                        "id": email,
                        "created_at": time.time(),
                        "domain": selected_domain,
                        "domain_health_snapshot": self._collect_domain_health_snapshot(specified_domain),
                    }
                    self._last_runtime_metrics = {
                        "service_type": self.service_type.value,
                        "selected_domain": selected_domain,
                        "create_email_status": "success",
                    }

                    logger.info(f"成功创建 Freemail 邮箱: {email}")
                    self.update_status(True)
                    return email_info
                except Exception as e:
                    last_error = e
                    self.update_status(False, e)
                    self._last_runtime_metrics = {
                        "service_type": self.service_type.value,
                        "selected_domain": selected_domain,
                        "create_email_status": "failed",
                        "create_email_error": str(e or ""),
                    }
                    self.report_registration_outcome(
                        f"probe@{selected_domain}",
                        success=False,
                        error_message=f"创建邮箱失败: {e}",
                    )
                    if idx < len(candidate_domains):
                        logger.warning(
                            "Freemail 使用域名 %s 创建邮箱失败，自动切换下一个域名重试（%s/%s）: %s",
                            selected_domain,
                            idx + 1,
                            len(candidate_domains),
                            str(e or ""),
                        )
                        continue

            if isinstance(last_error, EmailServiceError):
                raise last_error
            raise EmailServiceError(f"创建邮箱失败: {last_error}")

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
        poll_interval: int = 3,
    ) -> Optional[str]:
        """
        从 Freemail 邮箱获取验证码

        Args:
            email: 邮箱地址
            email_id: 未使用，保留接口兼容
            timeout: 超时时间（秒）
            pattern: 验证码正则
            otp_sent_at: OTP 发送时间戳，用于过滤旧邮件
            poll_interval: 轮询间隔（秒）

        Returns:
            验证码字符串，超时返回 None
        """
        logger.info(f"正在从 Freemail 邮箱 {email} 获取验证码...")

        start_time = time.time()
        stage_marker = f"{email}|{int(float(otp_sent_at or 0))}"
        seen_mail_ids = self._stage_seen_mail_ids.setdefault(stage_marker, set())
        last_used_mail_id = str(self._last_used_mail_ids.get(email) or "").strip()
        last_error: Optional[Exception] = None
        warned_payload_shape = False
        poll_count = 0
        unknown_ts_grace_seconds = max(6, int(min(timeout, max(poll_interval * 3, 6))))

        while time.time() - start_time < timeout:
            try:
                poll_count += 1
                response_data = self._make_request("GET", "/api/emails", params={"mailbox": email, "limit": 20})
                mails = _extract_mail_list(response_data)
                if not mails:
                    if response_data not in ([], None) and not warned_payload_shape:
                        logger.warning(
                            "Freemail 邮件列表返回了未识别结构: mailbox=%s payload_type=%s keys=%s",
                            email,
                            type(response_data).__name__,
                            list(response_data.keys())[:10] if isinstance(response_data, dict) else None,
                        )
                        warned_payload_shape = True
                    time.sleep(poll_interval)
                    continue

                def _mail_sort_key(mail: Dict[str, Any]) -> tuple:
                    raw_created_at = (
                        mail.get("created_at")
                        or mail.get("received_at")
                        or mail.get("date")
                        or mail.get("timestamp")
                    )
                    created_at = _to_timestamp(raw_created_at) or 0.0
                    mail_id = mail.get("id") or mail.get("mail_id") or mail.get("message_id") or 0
                    try:
                        mail_id_num = int(mail_id)
                    except Exception:
                        mail_id_num = 0
                    return (created_at, mail_id_num)

                mails = sorted(mails, key=_mail_sort_key, reverse=True)
                candidates: List[Dict[str, Any]] = []
                unknown_ts_candidates: List[Dict[str, Any]] = []

                for mail in mails:
                    mail_id = (
                        mail.get("id")
                        or mail.get("mail_id")
                        or mail.get("message_id")
                        or f"{mail.get('sender','')}|{mail.get('subject','')}|{mail.get('received_at') or mail.get('created_at') or mail.get('date') or mail.get('timestamp')}"
                    )
                    if mail_id in seen_mail_ids:
                        continue
                    if last_used_mail_id and str(mail_id) == last_used_mail_id:
                        continue

                    raw_created_at = (
                        mail.get("created_at")
                        or mail.get("received_at")
                        or mail.get("date")
                        or mail.get("timestamp")
                    )
                    created_at = _to_timestamp(raw_created_at)

                    # 新注册后会立刻进入“重新登录拿 token”流程。
                    # 这里对可解析时间戳的邮件做更严格过滤，只接受 OTP 发送之后的新邮件，
                    # 降低再次捡到注册阶段旧验证码的概率。
                    if otp_sent_at and created_at and created_at + 2 < otp_sent_at:
                        skew_seconds = otp_sent_at - created_at
                        if not (_looks_like_naive_datetime_string(raw_created_at) and skew_seconds >= 6 * 3600):
                            seen_mail_ids.add(mail_id)
                            continue

                    sender = str(mail.get("sender", "")).lower()
                    subject = str(mail.get("subject", ""))
                    preview = str(mail.get("preview", ""))
                    snippet = str(mail.get("snippet", "") or mail.get("text", ""))
                    
                    content = f"{sender}\n{subject}\n{preview}\n{snippet}"
                    
                    if "openai" not in content.lower():
                        continue

                    # 尝试直接使用 Freemail 提取的验证码
                    v_code = mail.get("verification_code")
                    if v_code:
                        code = str(v_code).strip()
                    else:
                        code = ""

                    # 如果没有直接提供，通过正则匹配 preview
                    if not code:
                        match = re.search(pattern, content)
                        if match:
                            code = match.group(1)

                    # 如果依然未找到，获取邮件详情进行匹配
                    if not code:
                        try:
                            detail = self._make_request("GET", f"/api/email/{mail_id}")
                            full_content = str(detail.get("content", "")) + "\n" + str(detail.get("html_content", ""))
                            match = re.search(pattern, full_content)
                            if match:
                                code = match.group(1)
                        except Exception as e:
                            logger.debug(f"获取 Freemail 邮件详情失败: {e}")

                    if not code:
                        seen_mail_ids.add(mail_id)
                        continue

                    candidate = {
                        "mail_id": str(mail_id),
                        "code": code,
                        "created_at": created_at,
                        "has_ts": created_at is not None,
                        "is_recent": bool(
                            otp_sent_at and (created_at is not None) and (created_at + 2 >= otp_sent_at)
                        ),
                    }
                    if otp_sent_at and created_at is None:
                        unknown_ts_candidates.append(candidate)
                    else:
                        candidates.append(candidate)

                elapsed = time.time() - start_time
                if otp_sent_at and (not candidates) and unknown_ts_candidates and elapsed < unknown_ts_grace_seconds:
                    time.sleep(poll_interval)
                    continue

                all_candidates = candidates + unknown_ts_candidates
                if all_candidates:
                    best = sorted(
                        all_candidates,
                        key=lambda item: (
                            1 if item.get("is_recent") else 0,
                            1 if item.get("has_ts") else 0,
                            float(item.get("created_at") or 0.0),
                        ),
                        reverse=True,
                    )[0]
                    best_mail_id = str(best["mail_id"])
                    seen_mail_ids.add(best_mail_id)
                    self._last_used_mail_ids[email] = best_mail_id
                    self._last_runtime_metrics = {
                        "service_type": self.service_type.value,
                        "mailbox_email": email,
                        "otp_poll_count": poll_count,
                        "mail_list_size": len(mails),
                        "seen_mail_count": len(seen_mail_ids),
                        "selected_mail_id": best_mail_id,
                        "selected_mail_has_timestamp": bool(best.get("has_ts")),
                        "selected_mail_is_recent": bool(best.get("is_recent")),
                        "otp_fetch_status": "success",
                    }
                    logger.info(
                        "从 Freemail 邮箱 %s 找到验证码: %s（mail_id=%s ts=%s recent=%s）",
                        email,
                        best["code"],
                        best_mail_id,
                        best.get("created_at"),
                        best.get("is_recent"),
                    )
                    self.update_status(True)
                    return str(best["code"])

            except Exception as e:
                last_error = e
                self._last_runtime_metrics = {
                    "service_type": self.service_type.value,
                    "mailbox_email": email,
                    "otp_poll_count": poll_count,
                    "seen_mail_count": len(seen_mail_ids),
                    "otp_fetch_status": "error",
                    "otp_fetch_error": str(e or ""),
                }
                logger.warning(f"检查 Freemail 邮件时出错: {email} - {e}")

            time.sleep(poll_interval)

        if last_error:
            logger.warning(f"等待 Freemail 验证码超时: {email}，最后一次错误: {last_error}")
        else:
            logger.warning(f"等待 Freemail 验证码超时: {email}")
        self._last_runtime_metrics = {
            "service_type": self.service_type.value,
            "mailbox_email": email,
            "otp_poll_count": poll_count,
            "seen_mail_count": len(seen_mail_ids),
            "otp_fetch_status": "timeout",
        }
        return None

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        """
        列出邮箱

        Args:
            **kwargs: 额外查询参数

        Returns:
            邮箱列表
        """
        try:
            params = {
                "limit": kwargs.get("limit", 100),
                "offset": kwargs.get("offset", 0)
            }
            resp = self._make_request("GET", "/api/mailboxes", params=params)
            
            emails = []
            if isinstance(resp, list):
                for mail in resp:
                    address = mail.get("address")
                    if address:
                        emails.append({
                            "id": address,
                            "service_id": address,
                            "email": address,
                            "created_at": mail.get("created_at"),
                            "raw_data": mail
                        })
            self.update_status(True)
            return emails
        except Exception as e:
            logger.warning(f"列出 Freemail 邮箱失败: {e}")
            self.update_status(False, e)
            return []

    def delete_email(self, email_id: str) -> bool:
        """
        删除邮箱
        """
        try:
            self._make_request("DELETE", "/api/mailboxes", params={"address": email_id})
            logger.info(f"已删除 Freemail 邮箱: {email_id}")
            self.update_status(True)
            return True
        except Exception as e:
            logger.warning(f"删除 Freemail 邮箱失败: {e}")
            self.update_status(False, e)
            return False

    def check_health(self) -> bool:
        """检查服务健康状态"""
        try:
            self._make_request("GET", "/api/domains")
            self.update_status(True)
            return True
        except Exception as e:
            logger.warning(f"Freemail 健康检查失败: {e}")
            self.update_status(False, e)
            return False
