import asyncio
from contextlib import contextmanager
from datetime import datetime

from src.web.routes import websocket as websocket_routes


class DummyWebSocket:
    def __init__(self):
        self.accepted = False
        self.closed = False
        self.close_code = None
        self.close_reason = None
        self.sent_messages = []
        self.cookies = {"webui_auth": "test"}

    async def accept(self):
        self.accepted = True

    async def close(self, code=None, reason=None):
        self.closed = True
        self.close_code = code
        self.close_reason = reason

    async def send_json(self, payload):
        self.sent_messages.append(payload)


def test_task_websocket_closes_immediately_for_terminal_task(monkeypatch):
    events = []

    class DummyTask:
        status = "deferred"
        result = {"email": "stale@example.com"}
        error_message = "等待重试"
        reason_code = "token_password_unconfirmed"
        next_retry_at = datetime(2026, 4, 4, 9, 30, 0)

    @contextmanager
    def fake_db():
        yield object()

    monkeypatch.setattr(websocket_routes, "is_websocket_authenticated", lambda websocket: True)
    monkeypatch.setattr(websocket_routes, "get_db", fake_db)
    monkeypatch.setattr(websocket_routes.crud, "get_registration_task", lambda db, task_uuid: DummyTask())
    monkeypatch.setattr(
        websocket_routes.task_manager,
        "register_websocket",
        lambda task_uuid, websocket: events.append(("register", task_uuid)),
    )
    monkeypatch.setattr(
        websocket_routes.task_manager,
        "unregister_websocket",
        lambda task_uuid, websocket: events.append(("unregister", task_uuid)),
    )

    websocket = DummyWebSocket()
    asyncio.run(websocket_routes.task_websocket(websocket, "task-terminal"))

    assert websocket.accepted is True
    assert websocket.closed is True
    assert websocket.close_code == 1000
    assert websocket.sent_messages == [
        {
            "type": "status",
            "task_uuid": "task-terminal",
            "status": "deferred",
            "email": "stale@example.com",
            "error_message": "等待重试",
            "reason_code": "token_password_unconfirmed",
            "reason_text": "账号状态未确认，登录密码当前被拒绝",
            "next_retry_at": "2026-04-04T09:30:00",
        }
    ]
    assert events == []
