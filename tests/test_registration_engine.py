import base64
import json

from src.config.constants import EmailServiceType, OPENAI_API_ENDPOINTS, OPENAI_PAGE_TYPES
from src.core.http_client import OpenAIHTTPClient
from src.core.openai.oauth import OAuthStart
from src.core.register import (
    REGISTRATION_DISALLOWED_SUBDOMAIN_MESSAGE,
    RegistrationEngine,
    RegistrationResult,
    SignupFormResult,
)
from src.services.base import BaseEmailService


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None, on_return=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}
        self.on_return = on_return

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class QueueSession:
    def __init__(self, steps):
        self.steps = list(steps)
        self.calls = []
        self.cookies = {}

    def get(self, url, **kwargs):
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._request("POST", url, **kwargs)

    def request(self, method, url, **kwargs):
        return self._request(method.upper(), url, **kwargs)

    def close(self):
        return None

    def _request(self, method, url, **kwargs):
        self.calls.append({
            "method": method,
            "url": url,
            "kwargs": kwargs,
        })
        if not self.steps:
            raise AssertionError(f"unexpected request: {method} {url}")
        expected_method, expected_url, response = self.steps.pop(0)
        assert method == expected_method
        assert url == expected_url
        if callable(response):
            response = response(self)
        if response.on_return:
            response.on_return(self)
        return response


class FakeEmailService(BaseEmailService):
    def __init__(self, codes):
        super().__init__(EmailServiceType.TEMPMAIL)
        self.codes = list(codes)
        self.otp_requests = []
        self.runtime_metrics = {}

    def create_email(self, config=None):
        return {
            "email": "tester@example.com",
            "service_id": "mailbox-1",
        }

    def get_verification_code(
        self,
        email,
        email_id=None,
        timeout=120,
        pattern=r"(?<!\d)(\d{6})(?!\d)",
        otp_sent_at=None,
        poll_interval=3,
    ):
        self.otp_requests.append({
            "email": email,
            "email_id": email_id,
            "timeout": timeout,
            "otp_sent_at": otp_sent_at,
            "poll_interval": poll_interval,
        })
        if not self.codes:
            raise AssertionError("no verification code queued")
        return self.codes.pop(0)

    def list_emails(self, **kwargs):
        return []

    def delete_email(self, email_id):
        return True

    def check_health(self):
        return True

    def get_runtime_metrics(self):
        return dict(self.runtime_metrics or {})


class FakeOAuthManager:
    def __init__(self):
        self.start_calls = 0
        self.callback_calls = []

    def start_oauth(self):
        self.start_calls += 1
        return OAuthStart(
            auth_url=f"https://auth.example.test/flow/{self.start_calls}",
            state=f"state-{self.start_calls}",
            code_verifier=f"verifier-{self.start_calls}",
            redirect_uri="http://localhost:1455/auth/callback",
        )

    def handle_callback(self, callback_url, expected_state, code_verifier):
        self.callback_calls.append({
            "callback_url": callback_url,
            "expected_state": expected_state,
            "code_verifier": code_verifier,
        })
        return {
            "account_id": "acct-1",
            "access_token": "access-1",
            "refresh_token": "refresh-1",
            "id_token": "id-1",
        }


class FailingEmailService(BaseEmailService):
    def __init__(self, message):
        super().__init__(EmailServiceType.DUCK_MAIL)
        self.message = message

    def create_email(self, config=None):
        raise RuntimeError(self.message)

    def get_verification_code(
        self,
        email,
        email_id=None,
        timeout=120,
        pattern=r"(?<!\d)(\d{6})(?!\d)",
        otp_sent_at=None,
        poll_interval=3,
    ):
        return None

    def list_emails(self, **kwargs):
        return []

    def delete_email(self, email_id):
        return True

    def check_health(self):
        return False


class FakeOpenAIClient:
    def __init__(self, sessions, sentinel_tokens):
        self._sessions = list(sessions)
        self._session_index = 0
        self._session = self._sessions[0]
        self._sentinel_tokens = list(sentinel_tokens)

    @property
    def session(self):
        return self._session

    def check_ip_location(self):
        return True, "US"

    def check_sentinel(self, did):
        if not self._sentinel_tokens:
            raise AssertionError("no sentinel token queued")
        return self._sentinel_tokens.pop(0)

    def close(self):
        if self._session_index + 1 < len(self._sessions):
            self._session_index += 1
            self._session = self._sessions[self._session_index]


def _workspace_cookie(workspace_id):
    payload = base64.urlsafe_b64encode(
        json.dumps({"workspaces": [{"id": workspace_id}]}).encode("utf-8")
    ).decode("ascii").rstrip("=")
    return f"{payload}.sig"


def _response_with_did(did):
    return DummyResponse(
        status_code=200,
        text="ok",
        on_return=lambda session: session.cookies.__setitem__("oai-did", did),
    )


def _response_with_login_cookies(workspace_id="ws-1", session_token="session-1"):
    def setter(session):
        session.cookies["oai-client-auth-session"] = _workspace_cookie(workspace_id)
        session.cookies["__Secure-next-auth.session-token"] = session_token

    return DummyResponse(status_code=200, payload={}, on_return=setter)


def test_check_sentinel_sends_non_empty_pow(monkeypatch):
    session = QueueSession([
        ("POST", OPENAI_API_ENDPOINTS["sentinel"], DummyResponse(payload={"token": "sentinel-token"})),
    ])
    client = OpenAIHTTPClient()
    client._session = session

    monkeypatch.setattr(
        "src.core.http_client.build_sentinel_pow_token",
        lambda user_agent: "gAAAAACpow-token",
    )

    token = client.check_sentinel("device-1")

    assert token == "sentinel-token"
    body = json.loads(session.calls[0]["kwargs"]["data"])
    assert body["id"] == "device-1"
    assert body["flow"] == "authorize_continue"
    assert body["p"] == "gAAAAACpow-token"


def test_run_registers_then_relogs_to_fetch_token():
    session_one = QueueSession([
        ("GET", "https://auth.example.test/flow/1", _response_with_did("did-1")),
        (
            "POST",
            OPENAI_API_ENDPOINTS["signup"],
            DummyResponse(payload={"page": {"type": OPENAI_PAGE_TYPES["PASSWORD_REGISTRATION"]}}),
        ),
        ("POST", OPENAI_API_ENDPOINTS["register"], DummyResponse(payload={})),
        ("GET", OPENAI_API_ENDPOINTS["send_otp"], DummyResponse(payload={})),
        ("POST", OPENAI_API_ENDPOINTS["validate_otp"], DummyResponse(payload={})),
        ("POST", OPENAI_API_ENDPOINTS["create_account"], DummyResponse(payload={})),
    ])
    session_two = QueueSession([
        ("GET", "https://auth.example.test/flow/2", _response_with_did("did-2")),
        (
            "POST",
            OPENAI_API_ENDPOINTS["signup"],
            DummyResponse(payload={"page": {"type": OPENAI_PAGE_TYPES["LOGIN_PASSWORD"]}}),
        ),
        (
            "POST",
            OPENAI_API_ENDPOINTS["password_verify"],
            DummyResponse(payload={"page": {"type": OPENAI_PAGE_TYPES["EMAIL_OTP_VERIFICATION"]}}),
        ),
        ("POST", OPENAI_API_ENDPOINTS["validate_otp"], _response_with_login_cookies()),
        (
            "POST",
            OPENAI_API_ENDPOINTS["select_workspace"],
            DummyResponse(payload={"continue_url": "https://auth.example.test/continue"}),
        ),
        (
            "GET",
            "https://auth.example.test/continue",
            DummyResponse(
                status_code=302,
                headers={"Location": "http://localhost:1455/auth/callback?code=code-2&state=state-2"},
            ),
        ),
    ])

    email_service = FakeEmailService(["123456", "654321"])
    engine = RegistrationEngine(email_service)
    fake_oauth = FakeOAuthManager()
    engine.http_client = FakeOpenAIClient([session_one, session_two], ["sentinel-1", "sentinel-2"])
    engine.oauth_manager = fake_oauth

    result = engine.run()

    assert result.success is True
    assert result.source == "register"
    assert result.workspace_id == "ws-1"
    assert result.session_token == "session-1"
    assert fake_oauth.start_calls == 2
    assert len(email_service.otp_requests) == 2
    assert all(item["otp_sent_at"] is not None for item in email_service.otp_requests)
    assert sum(1 for call in session_one.calls if call["url"] == OPENAI_API_ENDPOINTS["send_otp"]) == 1
    assert sum(1 for call in session_two.calls if call["url"] == OPENAI_API_ENDPOINTS["send_otp"]) == 0
    assert sum(1 for call in session_one.calls if call["url"] == OPENAI_API_ENDPOINTS["select_workspace"]) == 0
    assert sum(1 for call in session_two.calls if call["url"] == OPENAI_API_ENDPOINTS["select_workspace"]) == 1
    relogin_start_body = json.loads(session_two.calls[1]["kwargs"]["data"])
    assert relogin_start_body["screen_hint"] == "login"
    assert relogin_start_body["username"]["value"] == "tester@example.com"
    password_verify_body = json.loads(session_two.calls[2]["kwargs"]["data"])
    assert password_verify_body == {"password": result.password}
    assert result.metadata["token_acquired_via_relogin"] is True


def test_run_surfaces_create_email_error_details():
    engine = RegistrationEngine(FailingEmailService("Domain not found."))
    engine.http_client = FakeOpenAIClient([QueueSession([])], [])

    result = engine.run()

    assert result.success is False
    assert result.error_message == "创建邮箱失败: Domain not found."


def test_existing_account_login_uses_auto_sent_otp_without_manual_send():
    session = QueueSession([
        ("GET", "https://auth.example.test/flow/1", _response_with_did("did-1")),
        (
            "POST",
            OPENAI_API_ENDPOINTS["signup"],
            DummyResponse(payload={"page": {"type": OPENAI_PAGE_TYPES["EMAIL_OTP_VERIFICATION"]}}),
        ),
        ("POST", OPENAI_API_ENDPOINTS["validate_otp"], _response_with_login_cookies("ws-existing", "session-existing")),
        (
            "POST",
            OPENAI_API_ENDPOINTS["select_workspace"],
            DummyResponse(payload={"continue_url": "https://auth.example.test/continue-existing"}),
        ),
        (
            "GET",
            "https://auth.example.test/continue-existing",
            DummyResponse(
                status_code=302,
                headers={"Location": "http://localhost:1455/auth/callback?code=code-1&state=state-1"},
            ),
        ),
    ])

    email_service = FakeEmailService(["246810"])
    engine = RegistrationEngine(email_service)
    fake_oauth = FakeOAuthManager()
    engine.http_client = FakeOpenAIClient([session], ["sentinel-1"])
    engine.oauth_manager = fake_oauth

    result = engine.run()

    assert result.success is True
    assert result.source == "login"
    assert fake_oauth.start_calls == 1
    assert sum(1 for call in session.calls if call["url"] == OPENAI_API_ENDPOINTS["send_otp"]) == 0
    assert len(email_service.otp_requests) == 1
    assert email_service.otp_requests[0]["otp_sent_at"] is not None
    assert result.metadata["token_acquired_via_relogin"] is False


def test_verify_email_otp_retry_allows_same_code_after_new_send_window():
    email_service = FakeEmailService(["111111", "111111"])
    engine = RegistrationEngine(email_service)

    validations = []

    def fake_validate(code):
        validations.append(code)
        return len(validations) == 2

    send_calls = []

    def fake_send(referer=None):
        send_calls.append(referer)
        engine._otp_sent_at = 2000
        return True

    engine._validate_verification_code = fake_validate
    engine._send_verification_code = fake_send
    engine._otp_sent_at = 1000

    tried_codes = set()
    first_round = engine._verify_email_otp_with_retry(
        stage_label="登录验证码",
        max_attempts=1,
        fetch_timeout=1,
        attempted_codes=tried_codes,
    )
    resent = engine._send_verification_code(referer="https://auth.openai.com/email-verification")
    second_round = engine._verify_email_otp_with_retry(
        stage_label="登录验证码(原地重发)",
        max_attempts=1,
        fetch_timeout=1,
        attempted_codes=tried_codes,
    )

    assert first_round is False
    assert resent is True
    assert second_round is True
    assert validations == ["111111", "111111"]
    assert send_calls == ["https://auth.openai.com/email-verification"]


def test_login_otp_stops_current_round_on_wrong_code():
    email_service = FakeEmailService(["111111", "222222"])
    engine = RegistrationEngine(email_service)
    engine._otp_sent_at = 1000

    attempts = []

    def fake_validate(code):
        attempts.append(code)
        engine._last_otp_validation_error_code = "wrong_email_otp_code"
        engine._last_otp_validation_outcome = "http_non_200"
        return False

    engine._validate_verification_code = fake_validate

    ok = engine._verify_email_otp_with_retry(
        stage_label="登录验证码",
        max_attempts=3,
        fetch_timeout=10,
        attempted_codes=set(),
    )

    assert ok is False
    assert attempts == ["111111"]


def test_login_otp_stops_current_round_when_continue_url_returns_registration_gate():
    email_service = FakeEmailService(["111111"])
    engine = RegistrationEngine(email_service)
    engine._otp_sent_at = 1000
    engine._token_acquisition_requires_login = True

    def fake_validate(code):
        engine._last_otp_validation_error_code = "login_continue_url_registration_gate"
        engine._last_otp_validation_outcome = "semantic_reject"
        return False

    engine._validate_verification_code = fake_validate

    ok = engine._verify_email_otp_with_retry(
        stage_label="登录验证码(原地重发)",
        max_attempts=3,
        fetch_timeout=10,
        attempted_codes=set(),
    )

    assert ok is False


def test_otp_stops_current_round_on_max_check_attempts():
    email_service = FakeEmailService(["111111", "222222"])
    engine = RegistrationEngine(email_service)
    engine._otp_sent_at = 1000

    attempts = []

    def fake_validate(code):
        attempts.append(code)
        engine._last_otp_validation_error_code = "max_check_attempts"
        engine._last_otp_validation_outcome = "http_non_200"
        return False

    engine._validate_verification_code = fake_validate

    ok = engine._verify_email_otp_with_retry(
        stage_label="登录验证码(重发)",
        max_attempts=3,
        fetch_timeout=10,
        attempted_codes=set(),
    )

    assert ok is False
    assert attempts == ["111111"]


def test_native_backup_falls_back_to_oauth_authorize_when_workspace_continue_is_gate_url():
    engine = RegistrationEngine(FakeEmailService([]))
    engine.session = QueueSession([])
    fake_oauth = FakeOAuthManager()
    engine.oauth_manager = fake_oauth

    engine._last_validate_otp_workspace_id = "ws-gate"
    engine._last_validate_otp_continue_url = "https://auth.openai.com/about-you"
    engine._create_account_continue_url = "https://auth.openai.com/add-phone"

    redirect_starts = []

    engine._verify_email_otp_with_retry = lambda *args, **kwargs: True
    engine._get_workspace_id = lambda: "ws-gate"
    engine._select_workspace = lambda workspace_id: "https://auth.openai.com/add-phone"
    engine._follow_redirects = lambda start_url: (
        redirect_starts.append(start_url) or ("http://localhost:1455/auth/callback?code=code-3&state=state-1", start_url)
    )
    engine._handle_oauth_callback = lambda callback_url: {
        "account_id": "acct-gate",
        "access_token": "access-gate",
        "refresh_token": "refresh-gate",
        "id_token": "id-gate",
    }

    result = RegistrationResult(success=False, email="tester@example.com")

    ok = engine._complete_token_exchange_native_backup(result)

    assert ok is True
    assert fake_oauth.start_calls == 1
    assert redirect_starts == ["https://auth.example.test/flow/1"]
    assert result.workspace_id == "ws-gate"
    assert result.account_id == "acct-gate"
    assert result.access_token == "access-gate"


def test_native_backup_recovers_when_oauth_fallback_lands_on_login_page():
    engine = RegistrationEngine(FakeEmailService([]))
    engine.session = QueueSession([])
    fake_oauth = FakeOAuthManager()
    engine.oauth_manager = fake_oauth
    engine.password = "pw-1"

    engine._last_validate_otp_workspace_id = ""
    engine._last_validate_otp_continue_url = "https://auth.openai.com/about-you"
    engine._create_account_continue_url = "https://auth.openai.com/add-phone"

    engine._verify_email_otp_with_retry = lambda *args, **kwargs: True
    engine._get_workspace_id = lambda: ""
    engine._select_workspace = lambda workspace_id: ""
    engine._follow_redirects = lambda start_url: (None, "https://auth.openai.com/log-in")

    def fake_bridge(result):
        result.access_token = "bridge-access"
        result.session_token = "bridge-session"
        result.workspace_id = "bridge-ws"
        return True

    engine._bridge_login_for_session_token = fake_bridge

    result = RegistrationResult(success=False, email="tester@example.com")

    ok = engine._complete_token_exchange_native_backup(result)

    assert ok is True
    assert fake_oauth.start_calls == 1
    assert result.access_token == "bridge-access"
    assert result.session_token == "bridge-session"
    assert result.workspace_id == "bridge-ws"
    assert result.password == "pw-1"


def test_native_backup_restarts_full_login_flow_when_bridge_recovery_fails():
    engine = RegistrationEngine(FakeEmailService([]))
    engine.session = QueueSession([])
    fake_oauth = FakeOAuthManager()
    engine.oauth_manager = fake_oauth
    engine.password = "pw-2"
    engine._create_account_account_id = "acct-created"
    engine._create_account_workspace_id = "ws-created"
    engine._create_account_refresh_token = "refresh-created"

    follow_results = [
        (None, "https://auth.openai.com/log-in"),
        ("http://localhost:1455/auth/callback?code=code-2&state=state-2", "https://chatgpt.com/"),
    ]
    restart_calls = []
    verify_calls = []

    engine._verify_email_otp_with_retry = lambda *args, **kwargs: (verify_calls.append(kwargs.get("stage_label")) or True)
    engine._get_workspace_id = lambda: ""
    engine._select_workspace = lambda workspace_id: ""
    engine._follow_redirects = lambda start_url: follow_results.pop(0)
    engine._bridge_login_for_session_token = lambda result: False
    def fake_restart_login_flow():
        restart_calls.append(True)
        engine.oauth_start = fake_oauth.start_oauth()
        return True, ""

    engine._restart_login_flow = fake_restart_login_flow
    engine._handle_oauth_callback = lambda callback_url: {
        "account_id": "acct-recovered",
        "access_token": "access-recovered",
        "refresh_token": "refresh-recovered",
        "id_token": "id-recovered",
    }

    result = RegistrationResult(success=False, email="tester@example.com")

    ok = engine._complete_token_exchange_native_backup(result)

    assert ok is True
    assert restart_calls == [True]
    assert fake_oauth.start_calls == 2
    assert verify_calls == ["登录验证码", "登录验证码"]
    assert result.account_id == "acct-recovered"
    assert result.workspace_id == "ws-created"
    assert result.access_token == "access-recovered"
    assert result.refresh_token == "refresh-recovered"
    assert result.password == "pw-2"


def test_run_retries_with_fresh_email_when_create_account_reports_existing_user():
    engine = RegistrationEngine(FakeEmailService([]))
    engine._check_ip_location = lambda: (True, "US")

    created_emails = []

    def fake_create_email():
        idx = len(created_emails) + 1
        email = f"retry-{idx}@example.com"
        created_emails.append(email)
        engine.email = email
        engine.inbox_email = email
        engine.email_info = {"email": email, "service_id": email}
        return True

    create_account_outcomes = [
        (False, "user_already_exists", "An account already exists for this email address."),
        (True, "", ""),
    ]

    def fake_create_user_account():
        ok, code, message = create_account_outcomes.pop(0)
        engine._last_create_account_error_code = code
        engine._last_create_account_error_message = message
        return ok

    complete_calls = []

    engine._create_email = fake_create_email
    engine._prepare_authorize_flow = lambda label: ("did-1", "sentinel-1")
    engine._submit_signup_form = lambda did, sen: SignupFormResult(success=True)
    engine._register_password = lambda did, sen: (True, "pw-1")
    engine._send_verification_code = lambda referer=None: True
    engine._verify_email_otp_with_retry = lambda *args, **kwargs: True
    engine._create_user_account = fake_create_user_account
    engine._restart_login_flow = lambda: (True, "")

    def fake_complete(result):
        complete_calls.append(result.email)
        result.account_id = "acct-1"
        result.workspace_id = "ws-1"
        result.access_token = "access-1"
        result.password = engine.password or "pw-1"
        return True

    engine._complete_token_exchange_native_backup = fake_complete

    result = engine.run()

    assert result.success is True
    assert created_emails == ["retry-1@example.com", "retry-2@example.com"]
    assert result.email == "retry-2@example.com"
    assert complete_calls == ["retry-2@example.com"]


def test_run_preserves_registration_disallowed_reason_when_fresh_email_retries_exhausted():
    engine = RegistrationEngine(FakeEmailService([]))
    engine._check_ip_location = lambda: (True, "US")

    created_emails = []

    def fake_create_email():
        idx = len(created_emails) + 1
        email = f"retry-{idx}@example.com"
        created_emails.append(email)
        engine.email = email
        engine.inbox_email = email
        engine.email_info = {"email": email, "service_id": email}
        return True

    def fake_create_user_account():
        engine._last_create_account_error_code = "registration_disallowed"
        engine._last_create_account_error_message = (
            "Sorry, we cannot create your account with the given information."
        )
        return False

    engine._create_email = fake_create_email
    engine._prepare_authorize_flow = lambda label: ("did-1", "sentinel-1")
    engine._submit_signup_form = lambda did, sen: SignupFormResult(success=True)
    engine._register_password = lambda did, sen: (True, "pw-1")
    engine._send_verification_code = lambda referer=None: True
    engine._verify_email_otp_with_retry = lambda *args, **kwargs: True
    engine._create_user_account = fake_create_user_account

    result = engine.run()

    assert result.success is False
    assert result.reason_code == "registration_disallowed"
    assert result.error_message == f"{REGISTRATION_DISALLOWED_SUBDOMAIN_MESSAGE} 当前失效域名: example.com"
    assert created_emails == [
        "retry-1@example.com",
        "retry-2@example.com",
        "retry-3@example.com",
    ]
    assert any(
        REGISTRATION_DISALLOWED_SUBDOMAIN_MESSAGE in message
        for message in (result.logs or [])
    )


def test_registration_result_to_dict_includes_phase_and_reason_code():
    result = RegistrationResult(
        success=False,
        email="tester@example.com",
        error_message="等待验证码超时（15 秒）",
        phase="signup_otp_waiting",
        reason_code="email_otp_timeout",
    )

    payload = result.to_dict()

    assert payload["phase"] == "signup_otp_waiting"
    assert payload["reason_code"] == "email_otp_timeout"


def test_merge_email_service_runtime_metadata_includes_selected_domain_and_runtime_metrics():
    email_service = FakeEmailService([])
    email_service.runtime_metrics = {
        "otp_fetch_status": "success",
        "otp_poll_count": 2,
    }
    engine = RegistrationEngine(email_service)
    engine.email_info = {
        "email": "tester@example.com",
        "service_id": "mailbox-1",
        "domain": "a.example.com",
    }
    result = RegistrationResult(success=False)

    engine._merge_email_service_runtime_metadata(result)

    assert result.metadata["email_service_selected_domain"] == "a.example.com"
    assert result.metadata["email_service_runtime_metrics"]["otp_fetch_status"] == "success"
    assert result.metadata["email_service_runtime_metrics"]["otp_poll_count"] == 2
