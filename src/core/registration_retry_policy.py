from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from .registration_phases import fallback_resume_phase
from ..config.registration_retry_profiles import get_retry_profile


@dataclass
class RetryAction:
    should_defer: bool
    mark_dead: bool
    defer_bucket: str | None
    next_retry_at: datetime | None
    resume_phase: str | None
    should_rotate_email_context: bool = False
    should_rotate_identity_context: bool = False
    should_rotate_proxy_context: bool = False


def build_retry_action(
    email_service_type: str,
    reason_code: str,
    current_phase: str | None,
    retry_count: int,
    now: datetime | None = None,
) -> RetryAction:
    now = now or datetime.utcnow()
    profile = get_retry_profile(email_service_type)
    reason_policy = profile.get(reason_code) or profile.get("unknown_retryable")

    max_retry = int(reason_policy.get("max_retry", 0))
    if retry_count >= max_retry:
        return RetryAction(
            should_defer=False,
            mark_dead=True,
            defer_bucket=None,
            next_retry_at=None,
            resume_phase=None,
        )

    backoff = list(reason_policy.get("backoff") or [60])
    delay = int(backoff[min(retry_count, len(backoff) - 1)])
    bucket = reason_policy.get("bucket")
    resume_phase = fallback_resume_phase(reason_code, current_phase).value

    rotate_email = False
    rotate_identity = False
    rotate_proxy = False

    if reason_code == "registration_disallowed":
        rotate_email = True
        rotate_identity = True

    if reason_code == "primaryapi_server_error" and retry_count >= 1:
        rotate_proxy = True

    if reason_code == "email_otp_timeout" and retry_count >= 2:
        rotate_email = True

    return RetryAction(
        should_defer=True,
        mark_dead=False,
        defer_bucket=bucket,
        next_retry_at=now + timedelta(seconds=delay),
        resume_phase=resume_phase,
        should_rotate_email_context=rotate_email,
        should_rotate_identity_context=rotate_identity,
        should_rotate_proxy_context=rotate_proxy,
    )
