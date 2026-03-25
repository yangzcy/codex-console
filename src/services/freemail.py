"""
Freemail 邮箱服务实现
基于自部署 Cloudflare Worker 临时邮箱服务 (https://github.com/idinging/freemail)
"""

import re
import time
import logging
import random
import string
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse

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
        }
        self.config = {**default_config, **(config or {})}
        self.config["base_url"] = self.config["base_url"].rstrip("/")
        self.config["domain"] = _normalize_domain_list(self.config.get("domain"))

        http_config = RequestConfig(
            timeout=self.config["timeout"],
            max_retries=self.config["max_retries"],
        )
        self.http_client = HTTPClient(proxy_url=None, config=http_config)

        # 缓存 domain 列表
        self._domains = []
        # 按 OTP 阶段缓存已消费邮件，避免重试时反复捡回旧邮件
        self._stage_seen_mail_ids: Dict[str, set] = {}
        # 跨阶段记录最近一次真正消费过的邮件，避免登录阶段再次捡到注册阶段旧邮件
        self._last_used_mail_ids: Dict[str, str] = {}

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

        target_domain = random.choice(matched)
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
        domain_index = self._resolve_domain_index(req_config.get("domain"))
                    
        prefix = req_config.get("name")
        try:
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
            }

            logger.info(f"成功创建 Freemail 邮箱: {email}")
            self.update_status(True)
            return email_info

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"创建邮箱失败: {e}")

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
        unknown_ts_grace_seconds = max(6, int(min(timeout, max(poll_interval * 3, 6))))

        while time.time() - start_time < timeout:
            try:
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
                logger.warning(f"检查 Freemail 邮件时出错: {email} - {e}")

            time.sleep(poll_interval)

        if last_error:
            logger.warning(f"等待 Freemail 验证码超时: {email}，最后一次错误: {last_error}")
        else:
            logger.warning(f"等待 Freemail 验证码超时: {email}")
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
