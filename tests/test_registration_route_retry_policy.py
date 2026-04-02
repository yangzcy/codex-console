from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import asyncio

from src.core.register import RegistrationResult
from src.core.mailbox_registry import MailboxRegistry
from src.database import crud
from src.database.session import DatabaseSessionManager
from src.web.routes import registration as registration_routes


def _patch_registry_path(monkeypatch, path: Path) -> None:
    monkeypatch.setattr(
        MailboxRegistry,
        "_registry_path",
        classmethod(lambda cls: path),
    )


def test_apply_retry_policy_marks_task_deferred(monkeypatch):
    updates = []
    monkeypatch.setattr(
        registration_routes.task_manager,
        "update_status",
        lambda task_uuid, status, **kwargs: updates.append((task_uuid, status, kwargs)),
    )

    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "route_retry.db"
        registry_path = Path(tmpdir) / "mailbox_registry.json"
        _patch_registry_path(monkeypatch, registry_path)
        manager = DatabaseSessionManager(f"sqlite:///{db_path}")
        manager.create_tables()
        manager.migrate_tables()

        session = manager.SessionLocal()
        try:
            crud.create_registration_task(session, task_uuid="task-defer")
            result = RegistrationResult(
                success=False,
                email="deferred@example.com",
                error_message="等待验证码超时（15 秒）",
                phase="signup_otp_waiting",
                reason_code="email_otp_timeout",
            )

            outcome = registration_routes._apply_retry_policy_for_failed_task(
                session,
                "task-defer",
                result,
                "freemail",
            )
            task = crud.get_registration_task_by_uuid(session, "task-defer")

            assert outcome["outcome"] == "deferred"
            assert task.status == "deferred"
            assert task.reason_code == "email_otp_timeout"
            assert task.phase == "signup_otp_waiting"
            assert task.next_retry_at is not None
            assert updates[-1][1] == "deferred"
            registry_row = MailboxRegistry.get("deferred@example.com")
            assert registry_row is not None
            assert registry_row["state"] == "deferred_hold"
            assert registry_row["last_reason_code"] == "email_otp_timeout"
        finally:
            session.close()


def test_apply_retry_policy_marks_task_failed_after_max_retry(monkeypatch):
    updates = []
    monkeypatch.setattr(
        registration_routes.task_manager,
        "update_status",
        lambda task_uuid, status, **kwargs: updates.append((task_uuid, status, kwargs)),
    )

    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "route_retry_fail.db"
        registry_path = Path(tmpdir) / "mailbox_registry.json"
        _patch_registry_path(monkeypatch, registry_path)
        manager = DatabaseSessionManager(f"sqlite:///{db_path}")
        manager.create_tables()
        manager.migrate_tables()

        session = manager.SessionLocal()
        try:
            crud.create_registration_task(session, task_uuid="task-fail")
            crud.update_registration_task(
                session,
                "task-fail",
                retry_count=2,
            )
            result = RegistrationResult(
                success=False,
                email="blocked@example.com",
                error_message="We can't create your account due to our Terms of Use",
                phase="signup_start",
                reason_code="registration_disallowed",
            )

            outcome = registration_routes._apply_retry_policy_for_failed_task(
                session,
                "task-fail",
                result,
                "freemail",
            )
            task = crud.get_registration_task_by_uuid(session, "task-fail")

            assert outcome["outcome"] == "failed"
            assert task.status == "failed"
            assert task.reason_code == "registration_disallowed"
            assert updates[-1][1] == "failed"
            registry_row = MailboxRegistry.get("blocked@example.com")
            assert registry_row is not None
            assert registry_row["state"] == "invalid_hard"
            assert registry_row["last_reason_code"] == "registration_disallowed"
        finally:
            session.close()


def test_run_registration_task_skips_deferred_task_before_next_retry(monkeypatch):
    updates = []
    monkeypatch.setattr(
        registration_routes.task_manager,
        "update_status",
        lambda task_uuid, status, **kwargs: updates.append((task_uuid, status, kwargs)),
    )
    monkeypatch.setattr(
        registration_routes.task_manager,
        "add_log",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        registration_routes.task_manager,
        "get_loop",
        lambda: asyncio.get_event_loop(),
    )
    monkeypatch.setattr(
        registration_routes.task_manager,
        "set_loop",
        lambda loop: None,
    )

    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "route_retry_gate.db"
        manager = DatabaseSessionManager(f"sqlite:///{db_path}")
        manager.create_tables()
        manager.migrate_tables()

        session = manager.SessionLocal()
        try:
            crud.create_registration_task(session, task_uuid="task-gated")
            crud.update_registration_task_retry_state(
                session,
                "task-gated",
                status="deferred",
                reason_code="email_otp_timeout",
                next_retry_at=datetime.utcnow().replace(microsecond=0) + timedelta(seconds=60),
                phase="signup_otp_waiting",
            )
        finally:
            session.close()

        original_get_db = registration_routes.get_db
        registration_routes.get_db = lambda: manager.session_scope()
        try:
            outcome = asyncio.run(
                registration_routes.run_registration_task(
                    "task-gated",
                    "freemail",
                    None,
                    None,
                )
            )
        finally:
            registration_routes.get_db = original_get_db

        assert outcome["outcome"] == "deferred"
        assert updates[-1][1] == "deferred"


def test_gate_task_execution_allows_non_deferred_task():
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "route_retry_gate_pending.db"
        manager = DatabaseSessionManager(f"sqlite:///{db_path}")
        manager.create_tables()
        manager.migrate_tables()

        session = manager.SessionLocal()
        try:
            crud.create_registration_task(session, task_uuid="task-pending")
        finally:
            session.close()

        original_get_db = registration_routes.get_db
        registration_routes.get_db = lambda: manager.session_scope()
        try:
            can_run, outcome = registration_routes._gate_task_execution_by_retry_window("task-pending")
        finally:
            registration_routes.get_db = original_get_db

        assert can_run is True
        assert outcome is None


def test_task_to_response_includes_retry_state(monkeypatch):
    class DummyTask:
        id = 1
        task_uuid = "task-view"
        status = "deferred"
        email_service_id = None
        proxy = None
        logs = None
        result = {
            "metadata": {
                "email_service_selected_domain": "a.example.com",
                "email_service_runtime_metrics": {
                    "otp_fetch_status": "timeout",
                    "otp_poll_count": 3,
                },
            }
        }
        error_message = "等待验证码超时（15 秒）"
        phase = "signup_otp_waiting"
        reason_code = "email_otp_timeout"
        defer_bucket = "deferred_short"
        retry_count = 2
        next_retry_at = datetime(2026, 1, 1, 0, 1, 0)
        context_version = 1
        created_at = datetime(2026, 1, 1, 0, 0, 0)
        started_at = None
        completed_at = None

    monkeypatch.setattr(registration_routes, "_resolve_task_email_service", lambda task, db=None: (None, None))
    monkeypatch.setattr(registration_routes, "_resolve_task_email", lambda task: None)

    response = registration_routes.task_to_response(DummyTask())

    assert response.phase == "signup_otp_waiting"
    assert response.reason_code == "email_otp_timeout"
    assert response.defer_bucket == "deferred_short"
    assert response.retry_count == 2
    assert response.next_retry_at == "2026-01-01T00:01:00"
    assert response.context_version == 1
    assert response.email_service_selected_domain == "a.example.com"
    assert response.email_service_runtime_metrics["otp_fetch_status"] == "timeout"


def test_apply_batch_wait_deferred_task_state_keeps_short_retry_window(monkeypatch):
    updates = []
    monkeypatch.setattr(
        registration_routes.task_manager,
        "update_status",
        lambda task_uuid, status, **kwargs: updates.append((task_uuid, status, kwargs)),
    )

    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "route_batch_wait_retry.db"
        manager = DatabaseSessionManager(f"sqlite:///{db_path}")
        manager.create_tables()
        manager.migrate_tables()

        session = manager.SessionLocal()
        try:
            crud.create_registration_task(session, task_uuid="task-batch-defer")

            outcome = registration_routes._apply_batch_wait_deferred_task_state(
                session,
                "task-batch-defer",
                reason="验证码 15 秒内未到达，先让出并发位",
                defer_code="otp_wait_timeout",
                phase="signup_otp_waiting",
                email_service_type="freemail",
            )
            task = crud.get_registration_task_by_uuid(session, "task-batch-defer")

            assert outcome["outcome"] == "deferred"
            assert task.status == "deferred"
            assert task.reason_code == "email_otp_timeout"
            assert task.defer_bucket == "deferred_short"
            assert task.phase == "signup_otp_waiting"
            assert task.retry_count == 1
            assert task.next_retry_at is not None
            delta = (task.next_retry_at - datetime.utcnow()).total_seconds()
            assert 0 < delta <= registration_routes.BATCH_OTP_WAIT_RETRY_DELAY_SECONDS + 2
            assert updates[-1][1] == "deferred"
        finally:
            session.close()
