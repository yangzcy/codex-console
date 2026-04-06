from pathlib import Path

from src.core.mailbox_registry import MailboxRegistry


def _patch_registry_path(monkeypatch, path: Path) -> None:
    monkeypatch.setattr(
        MailboxRegistry,
        "_registry_path",
        classmethod(lambda cls: path),
    )


def test_mailbox_registry_tracks_created_registered_and_failed(monkeypatch, tmp_path):
    registry_path = tmp_path / "mailbox_registry.json"
    _patch_registry_path(monkeypatch, registry_path)

    MailboxRegistry.mark_created(
        email="User@example.com",
        service_type="cloud_mail",
        service_id="User@example.com",
        task_uuid="task-1",
    )
    created = MailboxRegistry.get("user@example.com")
    assert created is not None
    assert created["state"] == "in_use"
    assert created["service_type"] == "cloud_mail"

    MailboxRegistry.mark_deferred(
        email="user@example.com",
        service_type="cloud_mail",
        service_id="user@example.com",
        task_uuid="task-1",
        reason_code="email_otp_timeout",
        next_retry_at="2026-04-02T10:00:00",
    )
    deferred = MailboxRegistry.get("user@example.com")
    assert deferred is not None
    assert deferred["state"] == "deferred_hold"
    assert deferred["fail_count"] == 1
    assert deferred["last_reason_code"] == "email_otp_timeout"

    MailboxRegistry.mark_registered(
        email="user@example.com",
        service_type="cloud_mail",
        service_id="user@example.com",
        task_uuid="task-1",
    )
    registered = MailboxRegistry.get("user@example.com")
    assert registered is not None
    assert registered["state"] == "useful"
    assert registered["registered_at"]

    MailboxRegistry.mark_failed(
        email="user@example.com",
        service_type="cloud_mail",
        service_id="user@example.com",
        task_uuid="task-2",
        reason_code="registration_disallowed",
        hard_invalid=True,
    )
    failed = MailboxRegistry.get("user@example.com")
    assert failed is not None
    assert failed["state"] == "invalid_hard"
    assert failed["fail_count"] == 2
    assert failed["quarantined_at"]
