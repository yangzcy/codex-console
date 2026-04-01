from src.services.cloud_mail import CloudMailService
from src.services.base import EmailServiceError


def _make_service():
    return CloudMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_email": "admin@example.com",
            "admin_password": "secret",
            "domain": ["a.example.com", "b.example.com", "c.example.com", "d.example.com"],
            "exploratory_probe_slots": 2,
        }
    )


def _reset_cloud_mail_state():
    CloudMailService._runtime_domain_block_until = {}
    CloudMailService._domain_rotation_offsets = {}
    CloudMailService._domain_inflight_allocations = {}


def test_normalize_domain_list_accepts_urls_and_json_lists():
    assert CloudMailService._normalize_domain_list(
        '["https://A.example.com/path", "@b.example.com", "a.example.com"]'
    ) == ["a.example.com", "b.example.com"]


def test_report_registration_outcome_success_clears_runtime_cooldown(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    _reset_cloud_mail_state()

    service = _make_service()

    service.report_registration_outcome(
        "tester@a.example.com",
        success=False,
        error_message="registration_disallowed",
    )
    cooling_snapshot = service.get_domain_health_snapshot()
    assert cooling_snapshot["available_domains"] == ["b.example.com", "c.example.com", "d.example.com"]
    assert cooling_snapshot["domain_states"]["a.example.com"]["is_cooling"] is True

    service.report_registration_outcome("tester@a.example.com", success=True)

    snapshot = service.get_domain_health_snapshot()
    assert "a.example.com" in snapshot["available_domains"]
    assert snapshot["domain_states"]["a.example.com"]["is_cooling"] is False
    assert snapshot["domain_states"]["a.example.com"]["cooldown_until"] == 0


def test_report_registration_outcome_failure_sets_runtime_cooldown(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    _reset_cloud_mail_state()

    service = _make_service()
    service.report_registration_outcome(
        "tester@a.example.com",
        success=False,
        error_message="registration_disallowed",
    )

    runtime_key = "https://mail.example.com::a.example.com"
    assert runtime_key in CloudMailService._runtime_domain_block_until
    assert CloudMailService._runtime_domain_block_until[runtime_key] > 0


def test_get_candidate_domains_limits_exploratory_slots(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    _reset_cloud_mail_state()

    service = _make_service()

    service.report_registration_outcome("tester@a.example.com", success=True)
    service.report_registration_outcome("tester@b.example.com", success=True)

    candidates = service._get_candidate_domains()

    assert candidates[:2] == ["a.example.com", "b.example.com"]
    assert len(candidates) == 4
    assert set(candidates[2:]) == {"c.example.com", "d.example.com"}


def test_get_candidate_domains_respects_zero_exploratory_slots(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    _reset_cloud_mail_state()

    service = CloudMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_email": "admin@example.com",
            "admin_password": "secret",
            "domain": ["a.example.com", "b.example.com", "c.example.com"],
            "exploratory_probe_slots": 0,
        }
    )

    service.report_registration_outcome("tester@a.example.com", success=True)

    candidates = service._get_candidate_domains()

    assert candidates == ["a.example.com"]


def test_report_registration_outcome_success_releases_inflight(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    _reset_cloud_mail_state()

    service = _make_service()
    CloudMailService._increment_domain_inflight("https://mail.example.com", "a.example.com")

    service.report_registration_outcome("tester@a.example.com", success=True)

    snapshot = service.get_domain_health_snapshot()
    assert snapshot["domain_states"]["a.example.com"]["inflight_count"] == 0


def test_create_email_falls_back_and_records_domain_failure(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    _reset_cloud_mail_state()

    service = CloudMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_email": "admin@example.com",
            "admin_password": "secret",
            "domain": ["b.example.com", "https://a.example.com/path"],
        }
    )
    service.report_registration_outcome("probe@a.example.com", success=True)

    calls = []
    responses = [
        EmailServiceError("创建邮箱失败: upstream boom"),
        {"code": 200},
    ]

    def fake_make_request(method, path, **kwargs):
        payload = kwargs.get("json", {})
        email = payload.get("list", [{}])[0].get("email")
        calls.append(email)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(service, "_make_request", fake_make_request)
    monkeypatch.setattr(service, "_generate_password", lambda length=12: "pw-123")

    result = service.create_email({"name": "tester"})
    snapshot = service.get_domain_health_snapshot()

    assert result["email"] == "tester@b.example.com"
    assert calls == ["tester@a.example.com", "tester@b.example.com"]
    assert snapshot["domain_states"]["a.example.com"]["fail_count"] == 1
    assert snapshot["domain_states"]["a.example.com"]["last_outcome"] == "mailbox_create_failed"
    assert snapshot["domain_states"]["b.example.com"]["inflight_count"] == 1


def test_get_verification_code_skips_last_consumed_mail_across_stages(monkeypatch):
    _reset_cloud_mail_state()
    service = _make_service()

    responses = [
        {
            "code": 200,
            "data": [
                {
                    "emailId": "10",
                    "sendEmail": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 111111",
                    "createdAt": "2026-03-25T06:01:11+00:00",
                },
            ],
        },
        {
            "code": 200,
            "data": [
                {
                    "emailId": "10",
                    "sendEmail": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 111111",
                    "createdAt": "2026-03-25T06:01:11+00:00",
                },
                {
                    "emailId": "11",
                    "sendEmail": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 222222",
                    "createdAt": "2026-03-25T06:01:18+00:00",
                },
            ],
        },
    ]

    def fake_make_request(method, path, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(service, "_make_request", fake_make_request)

    code_1 = service.get_verification_code("tester@example.com", timeout=1, otp_sent_at=1000, poll_interval=0)
    code_2 = service.get_verification_code("tester@example.com", timeout=1, otp_sent_at=1005, poll_interval=0)

    assert code_1 == "111111"
    assert code_2 == "222222"
