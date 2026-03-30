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
