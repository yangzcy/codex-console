from datetime import datetime

from src.core.registration_retry_policy import build_retry_action


def test_build_retry_action_uses_freemail_profile():
    now = datetime(2026, 1, 1, 0, 0, 0)
    action = build_retry_action(
        email_service_type="freemail",
        reason_code="email_otp_timeout",
        current_phase="signup_otp_waiting",
        retry_count=0,
        now=now,
    )

    assert action.should_defer is True
    assert action.defer_bucket == "deferred_short"
    assert int((action.next_retry_at - now).total_seconds()) == 20
    assert action.resume_phase == "signup_otp_waiting"


def test_build_retry_action_uses_cloud_mail_profile():
    now = datetime(2026, 1, 1, 0, 0, 0)
    action = build_retry_action(
        email_service_type="cloud_mail",
        reason_code="email_otp_timeout",
        current_phase="signup_otp_waiting",
        retry_count=0,
        now=now,
    )

    assert action.should_defer is True
    assert int((action.next_retry_at - now).total_seconds()) == 30


def test_build_retry_action_rotates_identity_for_registration_disallowed():
    action = build_retry_action(
        email_service_type="freemail",
        reason_code="registration_disallowed",
        current_phase="profile_submitted",
        retry_count=0,
    )

    assert action.should_rotate_email_context is True
    assert action.should_rotate_identity_context is True
    assert action.resume_phase == "signup_otp_verified"


def test_build_retry_action_marks_dead_after_max_retry():
    action = build_retry_action(
        email_service_type="cloud_mail",
        reason_code="oauth_callback_miss",
        current_phase="oauth_finish",
        retry_count=2,
    )

    assert action.mark_dead is True
    assert action.should_defer is False
