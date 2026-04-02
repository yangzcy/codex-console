from src.services.freemail import FreemailService


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class FakeHTTPClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({
            "method": method,
            "url": url,
            "kwargs": kwargs,
        })
        if not self.responses:
            raise AssertionError(f"未准备响应: {method} {url}")
        return self.responses.pop(0)


def test_get_verification_code_accepts_dict_wrapped_emails_payload():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": "example.com",
    })
    service.http_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "emails": [
                    {
                        "id": 101,
                        "sender": "otp@tm1.openai.com",
                        "subject": "Your ChatGPT code is 123456",
                        "received_at": "2026-03-25 05:50:19",
                    }
                ]
            }
        )
    ])

    code = service.get_verification_code("tester@example.com", timeout=1, poll_interval=1)

    assert code == "123456"


def test_get_verification_code_ignores_large_naive_timestamp_skew():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": "example.com",
    })
    otp_sent_at = 1_742_881_736.0
    service.http_client = FakeHTTPClient([
        FakeResponse(
            payload=[
                {
                    "id": 202,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 654321",
                    "received_at": "2026-03-25 13:50:19",
                    "verification_code": "654321",
                }
            ]
        )
    ])

    code = service.get_verification_code(
        "tester@example.com",
        timeout=1,
        otp_sent_at=otp_sent_at,
        poll_interval=1,
    )

    assert code == "654321"


def test_get_verification_code_supports_missing_mail_id():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": "example.com",
    })
    service.http_client = FakeHTTPClient([
        FakeResponse(
            payload=[
                {
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 777888",
                    "preview": "Your ChatGPT code is 777888",
                }
            ]
        )
    ])

    code = service.get_verification_code("tester@example.com", timeout=1, poll_interval=1)

    assert code == "777888"


def test_get_verification_code_prefers_newest_mail_and_remembers_seen_ids_within_stage():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": "example.com",
    })
    service.http_client = FakeHTTPClient([
        FakeResponse(
            payload=[
                {
                    "id": 1,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 111111",
                    "received_at": "2026-03-25 06:01:02",
                    "verification_code": "111111",
                },
                {
                    "id": 2,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 222222",
                    "received_at": "2026-03-25 06:01:11",
                    "verification_code": "222222",
                },
            ]
        ),
        FakeResponse(
            payload=[
                {
                    "id": 1,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 111111",
                    "received_at": "2026-03-25 06:01:02",
                    "verification_code": "111111",
                },
                {
                    "id": 2,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 222222",
                    "received_at": "2026-03-25 06:01:11",
                    "verification_code": "222222",
                },
                {
                    "id": 3,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 333333",
                    "received_at": "2026-03-25 06:01:18",
                    "verification_code": "333333",
                },
            ]
        ),
    ])

    code_1 = service.get_verification_code("tester@example.com", timeout=1, otp_sent_at=1000, poll_interval=1)
    code_2 = service.get_verification_code("tester@example.com", timeout=1, otp_sent_at=1000, poll_interval=1)

    assert code_1 == "222222"
    assert code_2 == "333333"


def test_get_verification_code_skips_last_consumed_mail_across_stages():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": "example.com",
    })
    service.http_client = FakeHTTPClient([
        FakeResponse(
            payload=[
                {
                    "id": 10,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 111111",
                    "received_at": "2026-03-25 06:01:11",
                    "verification_code": "111111",
                },
            ]
        ),
        FakeResponse(
            payload=[
                {
                    "id": 10,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 111111",
                    "received_at": "2026-03-25 06:01:11",
                    "verification_code": "111111",
                },
                {
                    "id": 11,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 222222",
                    "received_at": "2026-03-25 06:01:18",
                    "verification_code": "222222",
                },
            ]
        ),
    ])

    code_1 = service.get_verification_code("tester@example.com", timeout=1, otp_sent_at=1000, poll_interval=1)
    code_2 = service.get_verification_code("tester@example.com", timeout=1, otp_sent_at=1005, poll_interval=1)

    assert code_1 == "111111"
    assert code_2 == "222222"


def test_resolve_domain_index_uses_round_robin_for_multiple_domains():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": ["b.example.com", "a.example.com"],
    })
    service._domains = ["a.example.com", "b.example.com", "c.example.com"]

    first = service._resolve_domain_index()
    second = service._resolve_domain_index()
    third = service._resolve_domain_index()

    assert first == 0
    assert second == 1
    assert third == 0


def test_report_registration_outcome_success_clears_runtime_cooldown(monkeypatch, tmp_path):
    health_path = tmp_path / "freemail_domain_health.json"
    monkeypatch.setattr(FreemailService, "_health_store_path", staticmethod(lambda: health_path))
    FreemailService._domain_rotation_offsets = {}
    FreemailService._runtime_domain_block_until = {}

    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": ["a.example.com"],
    })
    service._domains = ["a.example.com"]

    service.report_registration_outcome(
        "tester@a.example.com",
        success=False,
        error_message="registration_disallowed",
    )
    cooling_snapshot = service.get_domain_health_snapshot()
    assert cooling_snapshot["available_domains"] == []
    assert cooling_snapshot["cooldown_domains"][0]["domain"] == "a.example.com"

    service.report_registration_outcome("tester@a.example.com", success=True)

    snapshot = service.get_domain_health_snapshot()
    assert snapshot["available_domains"] == ["a.example.com"]
    assert snapshot["cooldown_domains"] == []
    assert snapshot["domain_states"]["a.example.com"]["is_cooling"] is False


def test_create_email_falls_back_to_next_candidate_and_records_domain_failure(monkeypatch, tmp_path):
    health_path = tmp_path / "freemail_domain_health.json"
    monkeypatch.setattr(FreemailService, "_health_store_path", staticmethod(lambda: health_path))
    FreemailService._domain_rotation_offsets = {}
    FreemailService._runtime_domain_block_until = {}

    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": ["a.example.com", "b.example.com"],
    })
    service._domains = ["a.example.com", "b.example.com"]
    service.http_client = FakeHTTPClient([
        FakeResponse(status_code=500, payload={"error": "boom"}, text='{"error":"boom"}'),
        FakeResponse(payload={"email": "tester@b.example.com"}),
    ])

    result = service.create_email()
    snapshot = service.get_domain_health_snapshot()

    assert result["email"] == "tester@b.example.com"
    assert [call["kwargs"].get("params", {}).get("domainIndex") for call in service.http_client.calls] == [0, 1]
    assert snapshot["domain_states"]["a.example.com"]["fail_count"] == 1
    assert snapshot["domain_states"]["a.example.com"]["last_outcome"] == "mailbox_create_failed"


def test_get_verification_code_records_runtime_metrics():
    service = FreemailService({
        "base_url": "https://mail.example.com",
        "admin_token": "token-123",
        "domain": "example.com",
    })
    service.http_client = FakeHTTPClient([
        FakeResponse(
            payload=[
                {
                    "id": 101,
                    "sender": "otp@tm1.openai.com",
                    "subject": "Your ChatGPT code is 123456",
                    "received_at": "2026-03-25 05:50:19",
                    "verification_code": "123456",
                }
            ]
        )
    ])

    code = service.get_verification_code("tester@example.com", timeout=1, poll_interval=1)
    metrics = service.get_runtime_metrics()

    assert code == "123456"
    assert metrics["otp_fetch_status"] == "success"
    assert metrics["mailbox_email"] == "tester@example.com"
    assert metrics["selected_mail_id"] == "101"
    assert metrics["otp_poll_count"] >= 1
