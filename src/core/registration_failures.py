from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class RegistrationReasonCode(str, Enum):
    EMAIL_OTP_TIMEOUT = "email_otp_timeout"
    OAUTH_CALLBACK_MISS = "oauth_callback_miss"
    PRIMARYAPI_SERVER_ERROR = "primaryapi_server_error"
    REGISTRATION_DISALLOWED = "registration_disallowed"
    NETWORK_TIMEOUT = "network_timeout"
    UNKNOWN_RETRYABLE = "unknown_retryable"
    FATAL = "fatal"


@dataclass
class RegistrationFailureDecision:
    reason_code: RegistrationReasonCode
    retryable: bool
    detail: str = ""


def classify_registration_failure(
    error_message: str,
    phase: Optional[str] = None,
    http_status: Optional[int] = None,
    payload: Optional[Any] = None,
) -> RegistrationFailureDecision:
    del phase, payload

    text = str(error_message or "").strip().lower()

    if "等待验证码超时" in text:
        return RegistrationFailureDecision(
            reason_code=RegistrationReasonCode.EMAIL_OTP_TIMEOUT,
            retryable=True,
            detail=error_message,
        )

    if "未命中 oauth 回调" in text or "重定向链回到了登录页" in text:
        return RegistrationFailureDecision(
            reason_code=RegistrationReasonCode.OAUTH_CALLBACK_MISS,
            retryable=True,
            detail=error_message,
        )

    if "primaryapi_server_error" in text or (http_status == 500 and "server had an error" in text):
        return RegistrationFailureDecision(
            reason_code=RegistrationReasonCode.PRIMARYAPI_SERVER_ERROR,
            retryable=True,
            detail=error_message,
        )

    if "registration_disallowed" in text or "cannot create your account with the given information" in text:
        return RegistrationFailureDecision(
            reason_code=RegistrationReasonCode.REGISTRATION_DISALLOWED,
            retryable=True,
            detail=error_message,
        )

    if "timed out" in text or "timeout" in text:
        return RegistrationFailureDecision(
            reason_code=RegistrationReasonCode.NETWORK_TIMEOUT,
            retryable=True,
            detail=error_message,
        )

    return RegistrationFailureDecision(
        reason_code=RegistrationReasonCode.UNKNOWN_RETRYABLE,
        retryable=True,
        detail=error_message,
    )
