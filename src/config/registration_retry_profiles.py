from __future__ import annotations


DEFAULT_RETRY_PROFILES = {
    "freemail": {
        "email_otp_timeout": {"bucket": "deferred_short", "backoff": [20, 45, 90], "max_retry": 5},
        "oauth_callback_miss": {"bucket": "deferred_medium", "backoff": [60, 180, 300], "max_retry": 3},
        "token_password_pending": {"bucket": "deferred_medium", "backoff": [90, 180, 300, 600], "max_retry": 4},
        "token_password_unconfirmed": {"bucket": "deferred_medium", "backoff": [90, 180, 300, 600], "max_retry": 3},
        "primaryapi_server_error": {"bucket": "deferred_short", "backoff": [30, 90, 180], "max_retry": 3},
        "registration_disallowed": {"bucket": "deferred_long", "backoff": [600, 1800], "max_retry": 2},
        "network_timeout": {"bucket": "deferred_short", "backoff": [15, 45, 120], "max_retry": 3},
        "proxy_pool_exhausted": {"bucket": "deferred_long", "backoff": [900, 1800, 3600], "max_retry": 3},
        "unknown_retryable": {"bucket": "deferred_short", "backoff": [30, 60, 120], "max_retry": 2},
    },
    "cloud_mail": {
        "email_otp_timeout": {"bucket": "deferred_short", "backoff": [30, 60, 120], "max_retry": 4},
        "oauth_callback_miss": {"bucket": "deferred_medium", "backoff": [60, 180], "max_retry": 2},
        "token_password_pending": {"bucket": "deferred_medium", "backoff": [90, 180, 300, 600], "max_retry": 4},
        "token_password_unconfirmed": {"bucket": "deferred_medium", "backoff": [90, 180, 300, 600], "max_retry": 3},
        "primaryapi_server_error": {"bucket": "deferred_short", "backoff": [30, 90, 180], "max_retry": 3},
        "registration_disallowed": {"bucket": "deferred_long", "backoff": [600, 1800], "max_retry": 2},
        "network_timeout": {"bucket": "deferred_short", "backoff": [20, 60, 120], "max_retry": 3},
        "proxy_pool_exhausted": {"bucket": "deferred_long", "backoff": [900, 1800, 3600], "max_retry": 3},
        "unknown_retryable": {"bucket": "deferred_short", "backoff": [30, 60, 120], "max_retry": 2},
    },
}


def get_retry_profile(email_service_type: str) -> dict:
    key = str(email_service_type or "").strip().lower()
    return DEFAULT_RETRY_PROFILES.get(key, DEFAULT_RETRY_PROFILES["freemail"])
