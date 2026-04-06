from __future__ import annotations

from enum import Enum


class RegistrationPhase(str, Enum):
    INIT = "init"
    SIGNUP_OTP_WAITING = "signup_otp_waiting"
    SIGNUP_OTP_VERIFIED = "signup_otp_verified"
    PROFILE_SUBMITTED = "profile_submitted"
    LOGIN_OTP_WAITING = "login_otp_waiting"
    LOGIN_OTP_VERIFIED = "login_otp_verified"
    OAUTH_FINISH = "oauth_finish"
    DONE = "done"


def normalize_phase(value: str | None) -> RegistrationPhase:
    if not value:
        return RegistrationPhase.INIT
    try:
        return RegistrationPhase(value)
    except Exception:
        return RegistrationPhase.INIT


def fallback_resume_phase(reason_code: str, current_phase: str | None) -> RegistrationPhase:
    phase = normalize_phase(current_phase)

    if reason_code == "email_otp_timeout":
        if phase == RegistrationPhase.LOGIN_OTP_WAITING:
            return RegistrationPhase.LOGIN_OTP_WAITING
        return RegistrationPhase.SIGNUP_OTP_WAITING

    if reason_code == "oauth_callback_miss":
        return RegistrationPhase.LOGIN_OTP_VERIFIED

    if reason_code in {"token_password_pending", "token_password_unconfirmed"}:
        return phase

    if reason_code == "registration_disallowed":
        return RegistrationPhase.SIGNUP_OTP_VERIFIED

    return phase
