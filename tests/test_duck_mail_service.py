from src.services.duck_mail import DuckMailService


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


def test_create_email_creates_account_and_fetches_token():
    service = DuckMailService({
        "base_url": "https://api.duckmail.test",
        "default_domain": "duckmail.sbs",
        "api_key": "dk_test_key",
        "password_length": 10,
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "hydra:member": [
                    {"id": "domain-1", "domain": "duckmail.sbs"},
                ],
                "hydra:view": {
                    "hydra:last": "/domains?page=1",
                },
            }
        ),
        FakeResponse(
            status_code=201,
            payload={
                "id": "account-1",
                "address": "tester@duckmail.sbs",
                "authType": "email",
            },
        ),
        FakeResponse(
            payload={
                "id": "account-1",
                "token": "token-123",
            }
        ),
    ])
    service.http_client = fake_client

    email_info = service.create_email()

    assert email_info["email"] == "tester@duckmail.sbs"
    assert email_info["service_id"] == "account-1"
    assert email_info["account_id"] == "account-1"
    assert email_info["token"] == "token-123"

    list_domains_call = fake_client.calls[0]
    assert list_domains_call["method"] == "GET"
    assert list_domains_call["url"] == "https://api.duckmail.test/domains"
    assert list_domains_call["kwargs"]["params"] == {"page": 1}
    assert list_domains_call["kwargs"]["headers"]["Authorization"] == "Bearer dk_test_key"

    create_call = fake_client.calls[1]
    assert create_call["method"] == "POST"
    assert create_call["url"] == "https://api.duckmail.test/accounts"
    assert create_call["kwargs"]["json"]["address"].endswith("@duckmail.sbs")
    assert len(create_call["kwargs"]["json"]["password"]) == 10
    assert create_call["kwargs"]["headers"]["Authorization"] == "Bearer dk_test_key"

    token_call = fake_client.calls[2]
    assert token_call["method"] == "POST"
    assert token_call["url"] == "https://api.duckmail.test/token"
    assert token_call["kwargs"]["json"] == {
        "address": "tester@duckmail.sbs",
        "password": email_info["password"],
    }


def test_get_verification_code_reads_message_detail_and_extracts_code():
    service = DuckMailService({
        "base_url": "https://api.duckmail.test",
        "default_domain": "duckmail.sbs",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "hydra:member": [
                    {"id": "domain-1", "domain": "duckmail.sbs"},
                ],
                "hydra:view": {
                    "hydra:last": "/domains?page=1",
                },
            }
        ),
        FakeResponse(
            status_code=201,
            payload={
                "id": "account-1",
                "address": "tester@duckmail.sbs",
                "authType": "email",
            },
        ),
        FakeResponse(
            payload={
                "id": "account-1",
                "token": "token-123",
            }
        ),
        FakeResponse(
            payload={
                "hydra:member": [
                    {
                        "id": "msg-1",
                        "from": {
                            "name": "OpenAI",
                            "address": "noreply@openai.com",
                        },
                        "subject": "Your verification code",
                        "createdAt": "2026-03-19T10:00:00Z",
                    }
                ]
            }
        ),
        FakeResponse(
            payload={
                "id": "msg-1",
                "text": "Your OpenAI verification code is 654321",
                "html": [],
            }
        ),
    ])
    service.http_client = fake_client

    email_info = service.create_email()
    code = service.get_verification_code(
        email=email_info["email"],
        email_id=email_info["service_id"],
        timeout=1,
    )

    assert code == "654321"

    messages_call = fake_client.calls[3]
    assert messages_call["method"] == "GET"
    assert messages_call["url"] == "https://api.duckmail.test/messages"
    assert messages_call["kwargs"]["headers"]["Authorization"] == "Bearer token-123"

    detail_call = fake_client.calls[4]
    assert detail_call["method"] == "GET"
    assert detail_call["url"] == "https://api.duckmail.test/messages/msg-1"
    assert detail_call["kwargs"]["headers"]["Authorization"] == "Bearer token-123"


def test_create_email_fails_when_default_domain_not_available():
    service = DuckMailService({
        "base_url": "https://api.duckmail.test",
        "default_domain": "missing.example",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "hydra:member": [
                    {"id": "domain-1", "domain": "duckmail.sbs"},
                ],
                "hydra:view": {
                    "hydra:last": "/domains?page=1",
                },
            }
        ),
    ])
    service.http_client = fake_client

    try:
        service.create_email()
    except Exception as exc:
        assert "DuckMail 域名不可用或未验证" in str(exc)
    else:
        raise AssertionError("应当因为域名不可用而失败")


def test_check_health_requires_configured_domain_to_exist():
    service = DuckMailService({
        "base_url": "https://api.duckmail.test",
        "default_domain": "private.example",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "hydra:member": [
                    {"id": "domain-1", "domain": "duckmail.sbs"},
                ],
                "hydra:view": {
                    "hydra:last": "/domains?page=1",
                },
            }
        ),
    ])
    service.http_client = fake_client

    assert service.check_health() is False
    assert "private.example" in str(service.last_error)


def test_init_normalizes_default_domain_from_url():
    service = DuckMailService({
        "base_url": "https://api.duckmail.test",
        "default_domain": "https://mail.example.com/some/path",
    })

    assert service.config["default_domain"] == ["mail.example.com"]


def test_create_email_accepts_multiple_default_domains():
    service = DuckMailService({
        "base_url": "https://api.duckmail.test",
        "default_domain": ["missing.example", "duckmail.sbs"],
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "hydra:member": [
                    {"id": "domain-1", "domain": "duckmail.sbs"},
                    {"id": "domain-2", "domain": "baldur.edu.kg"},
                ],
                "hydra:view": {
                    "hydra:last": "/domains?page=1",
                },
            }
        ),
        FakeResponse(
            status_code=201,
            payload={
                "id": "account-1",
                "address": "tester@duckmail.sbs",
            },
        ),
        FakeResponse(
            payload={
                "id": "account-1",
                "token": "token-123",
            }
        ),
    ])
    service.http_client = fake_client

    email_info = service.create_email()

    assert email_info["email"].endswith("@duckmail.sbs")
