from src.core.registration_failures import (
    RegistrationReasonCode,
    classify_registration_failure,
)


def test_classify_email_otp_timeout():
    result = classify_registration_failure("等待验证码超时（15 秒）")
    assert result.reason_code == RegistrationReasonCode.EMAIL_OTP_TIMEOUT
    assert result.retryable is True


def test_classify_oauth_callback_miss():
    result = classify_registration_failure("登录收尾失败: 未命中 OAuth 回调，最终落点 https://example.com")
    assert result.reason_code == RegistrationReasonCode.OAUTH_CALLBACK_MISS


def test_classify_registration_disallowed():
    result = classify_registration_failure(
        "Sorry, we cannot create your account with the given information. code=registration_disallowed"
    )
    assert result.reason_code == RegistrationReasonCode.REGISTRATION_DISALLOWED


def test_classify_primaryapi_server_error():
    result = classify_registration_failure(
        "The server had an error while processing your request. code=primaryapi_server_error",
        http_status=500,
    )
    assert result.reason_code == RegistrationReasonCode.PRIMARYAPI_SERVER_ERROR


def test_classify_token_password_pending():
    result = classify_registration_failure(
        "账号已确认进入登录链路，但登录密码暂未生效，先让出并发位 90s 后再试: 重新登录提交密码失败"
    )
    assert result.reason_code == RegistrationReasonCode.TOKEN_PASSWORD_PENDING


def test_classify_token_password_unconfirmed():
    result = classify_registration_failure(
        "账号状态尚未确认，登录密码当前被拒绝，先让出并发位 90s 后再试探测: 重新登录提交密码失败"
    )
    assert result.reason_code == RegistrationReasonCode.TOKEN_PASSWORD_UNCONFIRMED
