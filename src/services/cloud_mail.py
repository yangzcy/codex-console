"""
Cloud Mail 邮箱服务实现
基于 Cloudflare Workers 的邮箱服务 (https://doc.skymail.ink)
"""

import re
import time
import logging
import random
import string
import json
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from .base import BaseEmailService, EmailServiceError, EmailServiceType
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


def _to_timestamp(value: Any) -> Optional[float]:
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


class CloudMailService(BaseEmailService):
    """
    Cloud Mail 邮箱服务
    基于 Cloudflare Workers 的自部署邮箱服务
    """
    
    # 类变量：所有实例共享token（按base_url区分）
    _shared_tokens: Dict[str, tuple] = {}  # {base_url: (token, expires_at)}
    _token_lock = None  # 延迟初始化
    _domain_health_lock = None  # 延迟初始化
    _domain_create_lock = None  # 延迟初始化
    _domain_rotation_offsets: Dict[str, int] = {}
    _runtime_domain_block_until: Dict[str, float] = {}
    _domain_inflight_allocations: Dict[str, int] = {}

    @staticmethod
    def _emit_runtime_event(message: str, level: str = "info") -> None:
        if level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
        else:
            logger.info(message)

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        """
        初始化 Cloud Mail 服务

        Args:
            config: 配置字典，支持以下键:
                - base_url: API 基础地址 (必需)
                - admin_email: 管理员邮箱 (必需)
                - admin_password: 管理员密码 (必需)
                - domain: 邮箱域名 (可选，用于生成邮箱地址)
                - timeout: 请求超时时间，默认 30
                - max_retries: 最大重试次数，默认 3
                - proxy_url: 代理地址 (可选)
            name: 服务名称
        """
        super().__init__(EmailServiceType.CLOUD_MAIL, name)

        required_keys = ["base_url", "admin_email", "admin_password"]
        missing_keys = [key for key in required_keys if not (config or {}).get(key)]
        if missing_keys:
            raise ValueError(f"缺少必需配置: {missing_keys}")

        default_config = {
            "timeout": 30,
            "max_retries": 3,
            "proxy_url": None,
            "exploratory_probe_slots": 2,
        }
        self.config = {**default_config, **(config or {})}
        self.config["base_url"] = self.config["base_url"].rstrip("/")
        self.config["domain"] = self._normalize_domain_list(self.config.get("domain"))
        try:
            self.config["exploratory_probe_slots"] = max(0, int(self.config.get("exploratory_probe_slots", 2)))
        except (TypeError, ValueError):
            self.config["exploratory_probe_slots"] = 2

        # 创建 requests session
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        
        # 初始化类级别的锁（线程安全）
        if CloudMailService._token_lock is None:
            import threading
            CloudMailService._token_lock = threading.Lock()
        if CloudMailService._domain_health_lock is None:
            import threading
            CloudMailService._domain_health_lock = threading.Lock()
        if CloudMailService._domain_create_lock is None:
            import threading
            CloudMailService._domain_create_lock = threading.Lock()

        # 缓存邮箱信息（实例级别）
        self._created_emails: Dict[str, Dict[str, Any]] = {}
        self._stage_seen_mail_ids: Dict[str, set] = {}
        self._last_used_mail_ids: Dict[str, str] = {}
        self._last_runtime_metrics: Dict[str, Any] = {}

    @staticmethod
    def _health_store_path() -> Path:
        app_root = Path(__file__).resolve().parents[2]
        data_dir = app_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "cloud_mail_domain_health.json"

    @staticmethod
    def _normalize_domain_list(value: Any) -> List[str]:
        if isinstance(value, list):
            domains = [_normalize_domain(item) for item in value]
        else:
            text = str(value or "").strip()
            if text.startswith("[") and text.endswith("]"):
                try:
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

    @classmethod
    def _load_domain_health(cls) -> Dict[str, Any]:
        path = cls._health_store_path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("读取 Cloud Mail 域名健康池失败: %s", exc)
            return {}

    @classmethod
    def _save_domain_health(cls, payload: Dict[str, Any]) -> None:
        path = cls._health_store_path()
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)

    def _domain_health_bucket(self) -> Dict[str, Any]:
        base_url = str(self.config.get("base_url") or "").strip().lower()
        with CloudMailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(base_url, {"domains": {}})
            self._save_domain_health(payload)
            return bucket

    def _collect_domain_health_snapshot(self, requested_domain: Any = None) -> Dict[str, Any]:
        configured_domains = self._normalize_domain_list(
            requested_domain if requested_domain not in (None, "") else self.config.get("domain")
        )
        base_url = str(self.config.get("base_url") or "").strip().lower()
        now_ts = time.time()

        with CloudMailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(base_url, {"domains": {}})
            states = bucket.setdefault("domains", {})

            available_domains: List[str] = []
            cooldown_domains: List[Dict[str, Any]] = []
            domain_states: Dict[str, Dict[str, Any]] = {}

            for domain in configured_domains:
                state = states.setdefault(domain, {})
                persisted_until = float(state.get("cooldown_until") or 0.0)
                runtime_until = float(
                    CloudMailService._runtime_domain_block_until.get(f"{base_url}::{domain}") or 0.0
                )
                inflight_count = int(
                    CloudMailService._domain_inflight_allocations.get(f"{base_url}::{domain}") or 0
                )
                cooldown_until = max(persisted_until, runtime_until)
                domain_states[domain] = {
                    "success_count": int(state.get("success_count") or 0),
                    "fail_count": int(state.get("fail_count") or 0),
                    "consecutive_failures": int(state.get("consecutive_failures") or 0),
                    "register_create_account_retryable_count": int(
                        state.get("register_create_account_retryable_count") or 0
                    ),
                    "last_error": str(state.get("last_error") or "").strip(),
                    "last_outcome": str(state.get("last_outcome") or "").strip(),
                    "cooldown_until": cooldown_until,
                    "inflight_count": inflight_count,
                    "is_proven": int(state.get("success_count") or 0) > 0,
                    "is_cooling": cooldown_until > now_ts,
                }
                if cooldown_until > now_ts:
                    cooldown_domains.append(
                        {
                            "domain": domain,
                            "cooldown_until": cooldown_until,
                            "cooldown_until_iso": datetime.fromtimestamp(cooldown_until).isoformat(),
                            "remaining_seconds": max(0, int(cooldown_until - now_ts)),
                            "last_error": str(state.get("last_error") or "").strip(),
                            "last_outcome": str(state.get("last_outcome") or "").strip(),
                            "inflight_count": inflight_count,
                        }
                    )
                else:
                    available_domains.append(domain)

            self._save_domain_health(payload)

        cooldown_domains.sort(key=lambda item: (item.get("cooldown_until") or 0.0, item.get("domain") or ""))
        return {
            "service_type": self.service_type.value,
            "base_url": base_url,
            "configured_domains": configured_domains,
            "available_domains": available_domains,
            "cooldown_domains": cooldown_domains,
            "has_available_domains": bool(available_domains),
            "domain_states": domain_states,
        }

    @staticmethod
    def _domain_inflight_key(base_url: str, domain: str) -> str:
        return f"{base_url}::{domain}"

    @classmethod
    def _get_domain_inflight_count(cls, base_url: str, domain: str) -> int:
        return int(cls._domain_inflight_allocations.get(cls._domain_inflight_key(base_url, domain)) or 0)

    @classmethod
    def _increment_domain_inflight(cls, base_url: str, domain: str) -> None:
        key = cls._domain_inflight_key(base_url, domain)
        cls._domain_inflight_allocations[key] = cls._get_domain_inflight_count(base_url, domain) + 1

    @classmethod
    def _decrement_domain_inflight(cls, base_url: str, domain: str) -> None:
        key = cls._domain_inflight_key(base_url, domain)
        current = cls._get_domain_inflight_count(base_url, domain)
        if current <= 1:
            cls._domain_inflight_allocations.pop(key, None)
            return
        cls._domain_inflight_allocations[key] = current - 1

    @staticmethod
    def _domain_priority_key(state: Dict[str, Any]) -> tuple:
        success_count = int(state.get("success_count") or 0)
        consecutive_failures = int(state.get("consecutive_failures") or 0)
        fail_count = int(state.get("fail_count") or 0)
        retryable_count = int(state.get("register_create_account_retryable_count") or 0)
        updated_at = str(state.get("updated_at") or "")
        is_proven = 1 if success_count > 0 else 0
        # 优先级：
        # 1. 已成功验证过的域名优先
        # 2. 成功次数越多越优先
        # 3. 连续失败/建号400越少越优先
        # 4. 总失败越少越优先
        # 5. 更新时间更早的放前面，减少同一坏域名刚失败又立即再试
        return (
            -is_proven,
            -success_count,
            consecutive_failures,
            retryable_count,
            fail_count,
            updated_at,
        )

    @staticmethod
    def _bootstrap_domain_order(configured_domains: List[str], ordered_domains: List[str]) -> List[str]:
        return list(ordered_domains or [])

    def _select_domain(self, requested_domain: Any = None) -> str:
        configured_domains = self._normalize_domain_list(
            requested_domain if requested_domain not in (None, "") else self.config.get("domain")
        )
        if not configured_domains:
            raise EmailServiceError("未配置邮箱域名，无法生成邮箱地址")

        now_ts = time.time()
        base_url = str(self.config.get("base_url") or "").strip().lower()
        with CloudMailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(base_url, {"domains": {}})
            states = bucket.setdefault("domains", {})

            healthy_domains: List[str] = []
            cooled_domains: List[tuple[str, float]] = []
            for domain in configured_domains:
                state = states.setdefault(domain, {})
                cooldown_until = float(state.get("cooldown_until") or 0.0)
                if cooldown_until > now_ts:
                    cooled_domains.append((domain, cooldown_until))
                    continue
                healthy_domains.append(domain)

            self._save_domain_health(payload)

        candidate_domains = healthy_domains
        if not candidate_domains:
            cooled_domains.sort(key=lambda item: item[1])
            candidate_domains = [cooled_domains[0][0]]
            logger.warning(
                "Cloud Mail 域名池当前全部处于冷却，临时放行最早恢复域名: %s",
                candidate_domains[0],
            )

        rotation_key = f"{base_url}::{','.join(candidate_domains)}"
        next_offset = CloudMailService._domain_rotation_offsets.get(rotation_key, 0)
        selected_domain = candidate_domains[next_offset % len(candidate_domains)]
        CloudMailService._domain_rotation_offsets[rotation_key] = (next_offset + 1) % len(candidate_domains)
        return selected_domain

    def _get_candidate_domains(self, requested_domain: Any = None) -> List[str]:
        snapshot = self._collect_domain_health_snapshot(requested_domain)
        configured_domains = list(snapshot.get("configured_domains") or [])
        if not configured_domains:
            raise EmailServiceError("未配置邮箱域名，无法生成邮箱地址")
        healthy_domains = list(snapshot.get("available_domains") or [])
        if healthy_domains:
            base_url = str(self.config.get("base_url") or "").strip().lower()
            with CloudMailService._domain_health_lock:
                payload = self._load_domain_health()
                bucket = payload.setdefault(base_url, {"domains": {}})
                states = bucket.setdefault("domains", {})
                ordered = sorted(
                    healthy_domains,
                    key=lambda domain: self._domain_priority_key(states.get(domain, {})),
                )
                self._save_domain_health(payload)

            proven_domains = [d for d in ordered if int(states.get(d, {}).get("success_count") or 0) > 0]
            exploratory_domains = [d for d in ordered if d not in proven_domains]

            if proven_domains:
                # 有已验证成功域名时，优先把流量压给 proven 域名。
                # exploratory 仅放行少量“未被占用”的探测位，控制坏域名试探成本。
                available_exploratory = [
                    domain
                    for domain in exploratory_domains
                    if self._get_domain_inflight_count(base_url, domain) <= 0
                ]
                probe_slots = max(0, int(self.config.get("exploratory_probe_slots") or 0))
                if available_exploratory:
                    if probe_slots <= 0:
                        return proven_domains
                    rotation_key = f"{base_url}::{','.join(available_exploratory)}"
                    next_offset = CloudMailService._domain_rotation_offsets.get(rotation_key, 0)
                    rotated = available_exploratory[next_offset:] + available_exploratory[:next_offset]
                    CloudMailService._domain_rotation_offsets[rotation_key] = (
                        (next_offset + 1) % len(available_exploratory)
                    )
                    return proven_domains + rotated[:probe_slots]
                return proven_domains

            # 冷启动阶段：健康池被清空后，先只放一个引导域名做 canary。
            # 这里固定取健康排序后的首选域名，在出现 proven 域名前不放行其他新域名，
            # 避免首批并发把多个探索域名同时打坏；同时不要对“配置里的最后一个域名”产生偏置。
            bootstrap_order = self._bootstrap_domain_order(configured_domains, ordered)
            bootstrap_domain = bootstrap_order[0] if bootstrap_order else ordered[0]
            return [bootstrap_domain]
        return []

    def _cooldown_domain(
        self,
        domain: str,
        *,
        outcome: str,
        cooldown_seconds: int,
        error_message: str,
    ) -> None:
        base_url = str(self.config.get("base_url") or "").strip().lower()
        now_ts = time.time()
        now_iso = datetime.now().isoformat()
        with CloudMailService._domain_health_lock:
            self._decrement_domain_inflight(base_url, domain)
            payload = self._load_domain_health()
            bucket = payload.setdefault(base_url, {"domains": {}})
            state = bucket.setdefault("domains", {}).setdefault(domain, {})
            state["updated_at"] = now_iso
            state["last_error"] = str(error_message or "").strip()
            state["last_outcome"] = outcome
            state["fail_count"] = int(state.get("fail_count") or 0) + 1
            state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1
            state["cooldown_until"] = max(float(state.get("cooldown_until") or 0.0), now_ts + max(0, cooldown_seconds))
            CloudMailService._runtime_domain_block_until[f"{base_url}::{domain}"] = state["cooldown_until"]
            self._save_domain_health(payload)
        logger.warning(
            "Cloud Mail 域名进入冷却: %s, outcome=%s, cooldown=%ss",
            domain,
            outcome,
            cooldown_seconds,
        )

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
        if "非法邮箱域名" in text:
            return "mailbox_invalid_domain"
        if "创建邮箱失败" in text:
            return "mailbox_create_failed"
        if "invalid_auth_step" in text or "invalid authorization step" in text:
            return "auth_step_invalid"
        if "cf_service_unavailable" in text or "http 503" in text or "http 429" in text:
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

        base_url = str(self.config.get("base_url") or "").strip().lower()
        outcome = self._classify_domain_error(error_message)
        now_ts = time.time()
        now_iso = datetime.now().isoformat()

        with CloudMailService._domain_health_lock:
            payload = self._load_domain_health()
            bucket = payload.setdefault(base_url, {"domains": {}})
            state = bucket.setdefault("domains", {}).setdefault(domain, {})

            state["updated_at"] = now_iso
            state["last_error"] = str(error_message or "").strip()
            state["last_outcome"] = outcome
            self._decrement_domain_inflight(base_url, domain)

            if success:
                state["success_count"] = int(state.get("success_count") or 0) + 1
                state["consecutive_failures"] = 0
                state["consecutive_strong_failures"] = 0
                state["register_create_account_retryable_count"] = 0
                state["cooldown_until"] = 0
                CloudMailService._runtime_domain_block_until.pop(f"{base_url}::{domain}", None)
                self._save_domain_health(payload)
                return

            if outcome == "upstream_transient":
                state["last_transient_at"] = now_iso
                self._save_domain_health(payload)
                return

            state["fail_count"] = int(state.get("fail_count") or 0) + 1
            state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1

            cooldown_seconds = 0
            if outcome == "domain_blocked":
                state["strong_fail_count"] = int(state.get("strong_fail_count") or 0) + 1
                state["consecutive_strong_failures"] = int(state.get("consecutive_strong_failures") or 0) + 1
                cooldown_seconds = 12 * 3600
                if int(state.get("consecutive_strong_failures") or 0) >= 2:
                    cooldown_seconds = 24 * 3600
            elif outcome == "otp_timeout":
                state["otp_timeout_count"] = int(state.get("otp_timeout_count") or 0) + 1
                if int(state.get("consecutive_failures") or 0) >= 3:
                    cooldown_seconds = 30 * 60
            elif outcome == "mailbox_invalid_domain":
                state["mailbox_invalid_domain_count"] = int(state.get("mailbox_invalid_domain_count") or 0) + 1
                state["consecutive_strong_failures"] = int(state.get("consecutive_strong_failures") or 0) + 1
                cooldown_seconds = 24 * 3600
            elif outcome == "mailbox_create_failed":
                state["mailbox_create_fail_count"] = int(state.get("mailbox_create_fail_count") or 0) + 1
                if int(state.get("mailbox_create_fail_count") or 0) >= 3:
                    cooldown_seconds = 15 * 60
            elif outcome == "register_create_account_retryable":
                state["register_create_account_retryable_count"] = int(
                    state.get("register_create_account_retryable_count") or 0
                ) + 1
                success_count = int(state.get("success_count") or 0)
                consecutive_failures = int(state.get("consecutive_failures") or 0)
                retryable_count = int(state.get("register_create_account_retryable_count") or 0)
                if success_count <= 0:
                    # 新域名还没跑通过时，更严格：连续两次建号400就直接拉闸。
                    if consecutive_failures >= 2:
                        cooldown_seconds = 30 * 60
                    if retryable_count >= 4:
                        cooldown_seconds = max(cooldown_seconds, 12 * 3600)
                else:
                    # 已验证成功过的域名保留一定容错，但仍尽快止损。
                    if consecutive_failures >= 3:
                        cooldown_seconds = 15 * 60
                    elif consecutive_failures >= 2:
                        cooldown_seconds = 5 * 60
            else:
                state["consecutive_strong_failures"] = 0

            if cooldown_seconds > 0:
                state["cooldown_until"] = max(float(state.get("cooldown_until") or 0.0), now_ts + cooldown_seconds)
                CloudMailService._runtime_domain_block_until[f"{base_url}::{domain}"] = state["cooldown_until"]
                logger.warning(
                    "Cloud Mail 域名进入冷却: %s, outcome=%s, cooldown=%ss",
                    domain,
                    outcome,
                    cooldown_seconds,
                )

            self._save_domain_health(payload)

    def get_domain_health_snapshot(self) -> Dict[str, Any]:
        return self._collect_domain_health_snapshot()

    def get_runtime_metrics(self) -> Dict[str, Any]:
        return dict(self._last_runtime_metrics or {})

    def _generate_token(self) -> str:
        """
        生成身份令牌

        Returns:
            token 字符串

        Raises:
            EmailServiceError: 生成失败
        """
        url = f"{self.config['base_url']}/api/public/genToken"
        payload = {
            "email": self.config["admin_email"],
            "password": self.config["admin_password"]
        }

        try:
            response = self.session.post(
                url, 
                json=payload, 
                timeout=self.config["timeout"]
            )

            if response.status_code >= 400:
                error_msg = f"生成 token 失败: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = f"{error_msg} - {error_data}"
                except Exception:
                    error_msg = f"{error_msg} - {response.text[:200]}"
                raise EmailServiceError(error_msg)

            data = response.json()
            if data.get("code") != 200:
                raise EmailServiceError(f"生成 token 失败: {data.get('message', 'Unknown error')}")

            token = data.get("data", {}).get("token")
            if not token:
                raise EmailServiceError("生成 token 失败: 未返回 token")

            logger.info("Cloud Mail token 生成成功")
            return token

        except requests.RequestException as e:
            self.update_status(False, e)
            raise EmailServiceError(f"生成 token 失败: {e}")
        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"生成 token 失败: {e}")

    def _get_token(self, force_refresh: bool = False) -> str:
        """
        获取有效的 token（带缓存，所有实例共享）

        Args:
            force_refresh: 是否强制刷新

        Returns:
            token 字符串
        """
        base_url = self.config["base_url"]
        
        with CloudMailService._token_lock:
            # 检查共享缓存（token 有效期设为 1 小时）
            if not force_refresh and base_url in CloudMailService._shared_tokens:
                token, expires_at = CloudMailService._shared_tokens[base_url]
                if time.time() < expires_at:
                    return token

            # 生成新 token
            token = self._generate_token()
            expires_at = time.time() + 3600  # 1 小时后过期
            CloudMailService._shared_tokens[base_url] = (token, expires_at)
            self._emit_runtime_event("[CloudMail] Token 已刷新，后续实例将复用新 token")
            return token

    def _get_headers(self, token: Optional[str] = None) -> Dict[str, str]:
        """构造请求头"""
        if token is None:
            token = self._get_token()

        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @staticmethod
    def _is_auth_error_response(data: Any) -> bool:
        """判断 Cloud Mail 是否返回了业务层认证错误。"""
        if not isinstance(data, dict):
            return False

        message = str(data.get("message", ""))
        code = data.get("code")
        return (
            code in {401, 403}
            or "token验证失败" in message
            or ("token" in message.lower() and "失败" in message)
        )

    def _make_request(
        self,
        method: str,
        path: str,
        retry_on_auth_error: bool = True,
        **kwargs
    ) -> Any:
        """
        发送请求并返回 JSON 数据

        Args:
            method: HTTP 方法
            path: 请求路径（以 / 开头）
            retry_on_auth_error: 认证失败时是否重试
            **kwargs: 传递给 requests 的额外参数

        Returns:
            响应 JSON 数据

        Raises:
            EmailServiceError: 请求失败
        """
        url = f"{self.config['base_url']}{path}"
        kwargs.setdefault("headers", {})
        kwargs["headers"].update(self._get_headers())
        kwargs.setdefault("timeout", self.config["timeout"])

        try:
            response = self.session.request(method, url, **kwargs)

            if response.status_code >= 400:
                # 如果是认证错误且允许重试，刷新 token 后重试一次
                if response.status_code == 401 and retry_on_auth_error:
                    logger.warning("Cloud Mail 认证失败，尝试刷新 token")
                    kwargs["headers"].update(self._get_headers(self._get_token(force_refresh=True)))
                    response = self.session.request(method, url, **kwargs)

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
                data = response.json()
            except Exception:
                return {"raw_response": response.text}

            # Cloud Mail 有时会返回 HTTP 200，但在业务层提示 token 失效
            if retry_on_auth_error and self._is_auth_error_response(data):
                logger.warning("Cloud Mail 返回业务层 token 失效，尝试刷新 token")
                kwargs["headers"].update(self._get_headers(self._get_token(force_refresh=True)))
                response = self.session.request(method, url, **kwargs)
                try:
                    data = response.json()
                except Exception:
                    return {"raw_response": response.text}

            return data

        except requests.RequestException as e:
            self.update_status(False, e)
            raise EmailServiceError(f"请求失败: {method} {path} - {e}")
        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"请求失败: {method} {path} - {e}")

    def _generate_email_address(self, prefix: Optional[str] = None, domain: Optional[str] = None) -> str:
        """
        生成邮箱地址

        Args:
            prefix: 邮箱前缀，如果不提供则随机生成
            domain: 指定域名，如果不提供则从配置中选择

        Returns:
            完整的邮箱地址
        """
        if not prefix:
            # 生成随机前缀：首字母 + 9位随机字符（共10位）
            first = random.choice(string.ascii_lowercase)
            rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=9))
            prefix = f"{first}{rest}"

        if not domain:
            domain = self._select_domain()

        return f"{prefix}@{domain}"

    def _generate_password(self, length: int = 12) -> str:
        """生成随机密码"""
        alphabet = string.ascii_letters + string.digits
        return "".join(random.choices(alphabet, k=length))

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        创建新邮箱地址

        Args:
            config: 配置参数:
                - name: 邮箱前缀（可选）
                - password: 邮箱密码（可选，不提供则自动生成）
                - domain: 邮箱域名（可选，覆盖默认域名）

        Returns:
            包含邮箱信息的字典:
            - email: 邮箱地址
            - service_id: 邮箱地址（用作标识）
            - password: 邮箱密码
        """
        req_config = config or {}

        prefix = req_config.get("name")
        specified_domain = req_config.get("domain")
        with CloudMailService._domain_create_lock:
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
                email_address = self._generate_email_address(prefix, selected_domain)
                password = req_config.get("password") or self._generate_password()
                payload = {
                    "list": [
                        {
                            "email": email_address,
                            "password": password
                        }
                    ]
                }

                try:
                    result = self._make_request("POST", "/api/public/addUser", json=payload)

                    if result.get("code") != 200:
                        raise EmailServiceError(f"创建邮箱失败: {result.get('message', 'Unknown error')}")

                    email_info = {
                        "email": email_address,
                        "service_id": email_address,
                        "id": email_address,
                        "password": password,
                        "created_at": time.time(),
                        "domain": selected_domain,
                        "domain_health_snapshot": self._collect_domain_health_snapshot(specified_domain),
                    }
                    self._last_runtime_metrics = {
                        "service_type": self.service_type.value,
                        "selected_domain": selected_domain,
                        "create_email_status": "success",
                    }

                    self._created_emails[email_address] = email_info
                    with CloudMailService._domain_health_lock:
                        self._increment_domain_inflight(
                            str(self.config.get("base_url") or "").strip().lower(),
                            selected_domain,
                        )
                    logger.info(f"成功创建 Cloud Mail 邮箱: {email_address}")
                    self.update_status(True)
                    return email_info

                except Exception as e:
                    last_error = e
                    error_text = str(e or "")
                    self._last_runtime_metrics = {
                        "service_type": self.service_type.value,
                        "selected_domain": selected_domain,
                        "create_email_status": "failed",
                        "create_email_error": error_text,
                    }
                    if "非法邮箱域名" in error_text:
                        self._cooldown_domain(
                            selected_domain,
                            outcome="mailbox_invalid_domain",
                            cooldown_seconds=24 * 3600,
                            error_message=error_text,
                        )
                        if idx < len(candidate_domains):
                            logger.warning(
                                "Cloud Mail 域名 %s 创建邮箱时报非法域名，自动切换下一个域名重试（%s/%s）",
                                selected_domain,
                                idx + 1,
                                len(candidate_domains),
                            )
                            continue
                    self.report_registration_outcome(
                        f"probe@{selected_domain}",
                        success=False,
                        error_message=f"创建邮箱失败: {error_text}",
                    )
                    self.update_status(False, e)
                    if idx < len(candidate_domains):
                        logger.warning(
                            "Cloud Mail 使用域名 %s 创建邮箱失败，自动切换下一个域名重试（%s/%s）: %s",
                            selected_domain,
                            idx + 1,
                            len(candidate_domains),
                            error_text,
                        )
                        continue

            self.update_status(False, last_error)
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
        从 Cloud Mail 邮箱获取验证码

        Args:
            email: 邮箱地址
            email_id: 未使用，保留接口兼容
            timeout: 超时时间（秒）
            pattern: 验证码正则
            otp_sent_at: OTP 发送时间戳
            poll_interval: 轮询间隔（秒）

        Returns:
            验证码字符串，超时返回 None
        """
        logger.info(f"正在从 Cloud Mail 邮箱 {email} 获取验证码...")

        start_time = time.time()
        stage_marker = f"{email}|{int(float(otp_sent_at or 0))}"
        seen_ids = self._stage_seen_mail_ids.setdefault(stage_marker, set())
        last_used_mail_id = str(self._last_used_mail_ids.get(email) or "").strip()
        last_error: Optional[Exception] = None
        check_count = 0
        unknown_ts_grace_seconds = max(6, int(min(timeout, max(poll_interval * 3, 6))))

        while time.time() - start_time < timeout:
            try:
                check_count += 1
                
                # 查询邮件列表
                url_path = "/api/public/emailList"
                payload = {
                    "toEmail": email,
                    "timeSort": "desc"  # 最新的邮件优先
                }

                result = self._make_request("POST", url_path, json=payload)

                if result.get("code") != 200:
                    time.sleep(poll_interval)
                    continue

                emails = result.get("data", [])
                if not isinstance(emails, list):
                    time.sleep(poll_interval)
                    continue

                def _mail_sort_key(mail: Dict[str, Any]) -> tuple:
                    raw_created_at = (
                        mail.get("createdAt")
                        or mail.get("created_at")
                        or mail.get("receivedAt")
                        or mail.get("received_at")
                        or mail.get("date")
                        or mail.get("timestamp")
                    )
                    created_at = _to_timestamp(raw_created_at) or 0.0
                    mail_id = mail.get("emailId") or mail.get("id") or mail.get("messageId") or 0
                    try:
                        mail_id_num = int(mail_id)
                    except Exception:
                        mail_id_num = 0
                    return (created_at, mail_id_num)

                emails = sorted(emails, key=_mail_sort_key, reverse=True)
                candidates: List[Dict[str, Any]] = []
                unknown_ts_candidates: List[Dict[str, Any]] = []

                for email_item in emails:
                    email_id = (
                        email_item.get("emailId")
                        or email_item.get("id")
                        or email_item.get("messageId")
                        or f"{email_item.get('sendEmail','')}|{email_item.get('subject','')}|"
                        f"{email_item.get('createdAt') or email_item.get('receivedAt') or email_item.get('timestamp')}"
                    )

                    if email_id in seen_ids:
                        continue
                    if last_used_mail_id and str(email_id) == last_used_mail_id:
                        continue

                    raw_created_at = (
                        email_item.get("createdAt")
                        or email_item.get("created_at")
                        or email_item.get("receivedAt")
                        or email_item.get("received_at")
                        or email_item.get("date")
                        or email_item.get("timestamp")
                    )
                    created_at = _to_timestamp(raw_created_at)
                    if otp_sent_at and created_at and created_at + 2 < otp_sent_at:
                        skew_seconds = otp_sent_at - created_at
                        if not (_looks_like_naive_datetime_string(raw_created_at) and skew_seconds >= 6 * 3600):
                            seen_ids.add(email_id)
                            continue

                    sender_email = str(email_item.get("sendEmail", "")).lower()
                    sender_name = str(email_item.get("sendName", "")).lower()
                    subject = str(email_item.get("subject", ""))
                    content = str(email_item.get("content", ""))
                    clean_content = re.sub(r"<[^>]+>", " ", content)
                    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                    clean_content = re.sub(email_pattern, "", clean_content)
                    search_text = "\n".join([sender_email, sender_name, subject, clean_content])

                    if "openai" not in search_text.lower():
                        seen_ids.add(email_id)
                        continue

                    match = re.search(pattern, subject) or re.search(pattern, clean_content)
                    if not match:
                        seen_ids.add(email_id)
                        continue

                    candidate = {
                        "mail_id": str(email_id),
                        "code": match.group(1),
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
                    seen_ids.add(best_mail_id)
                    self._last_used_mail_ids[email] = best_mail_id
                    code = str(best["code"])
                    self._last_runtime_metrics = {
                        "service_type": self.service_type.value,
                        "mailbox_email": email,
                        "otp_poll_count": check_count,
                        "mail_list_size": len(emails),
                        "seen_mail_count": len(seen_ids),
                        "selected_mail_id": best_mail_id,
                        "selected_mail_has_timestamp": bool(best.get("has_ts")),
                        "selected_mail_is_recent": bool(best.get("is_recent")),
                        "otp_fetch_status": "success",
                    }
                    self._emit_runtime_event(
                        f"[CloudMail] 找到验证码: {code} (email={email}, mail_id={best_mail_id})"
                    )
                    self.update_status(True)
                    return code

            except Exception as e:
                last_error = e
                self._last_runtime_metrics = {
                    "service_type": self.service_type.value,
                    "mailbox_email": email,
                    "otp_poll_count": check_count,
                    "seen_mail_count": len(seen_ids),
                    "otp_fetch_status": "error",
                    "otp_fetch_error": str(e or ""),
                }
                self._emit_runtime_event(f"[CloudMail] 拉取验证码异常: {e}", "warning")
                # 如果是认证错误，强制刷新token
                if "401" in str(e) or "认证" in str(e):
                    self._emit_runtime_event("[CloudMail] 检测到认证异常，尝试强制刷新 token", "warning")
                    try:
                        self._get_token(force_refresh=True)
                    except Exception as refresh_error:
                        self._emit_runtime_event(
                            f"[CloudMail] 强制刷新 token 失败: {refresh_error}",
                            "error",
                        )
                logger.error(f"检查邮件时出错: {e}", exc_info=True)

            time.sleep(poll_interval)

        if last_error:
            logger.warning(f"等待 Cloud Mail 验证码超时: {email}，最后一次错误: {last_error}")
        self._emit_runtime_event(
            f"[CloudMail] 等待验证码超时: email={email}, 检查次数={check_count}, 已处理邮件数={len(seen_ids)}",
            "warning",
        )
        self._last_runtime_metrics = {
            "service_type": self.service_type.value,
            "mailbox_email": email,
            "otp_poll_count": check_count,
            "seen_mail_count": len(seen_ids),
            "otp_fetch_status": "timeout",
        }
        return None

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        """
        列出已创建的邮箱（从缓存中获取）

        Returns:
            邮箱列表
        """
        return list(self._created_emails.values())

    def delete_email(self, email_id: str) -> bool:
        """
        删除邮箱（Cloud Mail API 不支持删除用户，仅从缓存中移除）

        Args:
            email_id: 邮箱地址

        Returns:
            是否删除成功
        """
        if email_id in self._created_emails:
            del self._created_emails[email_id]
            logger.info(f"已从缓存中移除 Cloud Mail 邮箱: {email_id}")
            return True

        logger.warning(f"Cloud Mail 邮箱不在缓存中: {email_id}")
        return False

    def check_health(self) -> bool:
        """检查服务健康状态"""
        try:
            # 尝试生成 token
            self._get_token(force_refresh=True)
            self.update_status(True)
            return True
        except Exception as e:
            logger.warning(f"Cloud Mail 健康检查失败: {e}")
            self.update_status(False, e)
            return False

    def get_email_messages(self, email_id: str, **kwargs) -> List[Dict[str, Any]]:
        """
        获取邮箱中的邮件列表

        Args:
            email_id: 邮箱地址
            **kwargs: 额外参数（如 timeSort）

        Returns:
            邮件列表
        """
        try:
            url_path = "/api/public/emailList"
            payload = {
                "toEmail": email_id,
                "timeSort": kwargs.get("timeSort", "desc")
            }

            result = self._make_request("POST", url_path, json=payload)

            if result.get("code") != 200:
                logger.warning(f"获取邮件列表失败: {result.get('message')}")
                return []

            self.update_status(True)
            return result.get("data", [])

        except Exception as e:
            logger.error(f"获取 Cloud Mail 邮件列表失败: {email_id} - {e}")
            self.update_status(False, e)
            return []

    def get_service_info(self) -> Dict[str, Any]:
        """获取服务信息"""
        return {
            "service_type": self.service_type.value,
            "name": self.name,
            "base_url": self.config["base_url"],
            "admin_email": self.config["admin_email"],
            "domain": self.config.get("domain"),
            "cached_emails_count": len(self._created_emails),
            "status": self.status.value,
        }
