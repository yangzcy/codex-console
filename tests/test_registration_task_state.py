from datetime import datetime, timedelta

from src.database.crud import (
    create_registration_task,
    update_registration_task_phase,
    update_registration_task_retry_state,
)
from src.database.session import DatabaseSessionManager


def test_registration_task_retry_fields_are_persisted(tmp_path):
    db_path = tmp_path / "task_state.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    manager.create_tables()
    manager.migrate_tables()

    session = manager.SessionLocal()
    try:
        task = create_registration_task(session, task_uuid="task-1")
        next_retry_at = datetime.utcnow() + timedelta(seconds=30)

        update_registration_task_phase(session, "task-1", "signup_otp_waiting")
        updated = update_registration_task_retry_state(
            session,
            "task-1",
            status="deferred",
            reason_code="email_otp_timeout",
            defer_bucket="deferred_short",
            retry_count=1,
            next_retry_at=next_retry_at,
            phase="signup_otp_waiting",
            context_version=2,
        )

        assert task.id is not None
        assert updated.phase == "signup_otp_waiting"
        assert updated.reason_code == "email_otp_timeout"
        assert updated.defer_bucket == "deferred_short"
        assert updated.retry_count == 1
        assert updated.context_version == 2
        assert updated.next_retry_at is not None
    finally:
        session.close()
