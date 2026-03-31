from src.services.cloud_mail import CloudMailService


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


def test_report_registration_outcome_success_clears_runtime_cooldown(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    CloudMailService._runtime_domain_block_until = {}
    CloudMailService._domain_rotation_offsets = {}
    CloudMailService._domain_inflight_allocations = {}

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


def test_get_candidate_domains_limits_exploratory_slots(monkeypatch, tmp_path):
    health_path = tmp_path / "cloud_mail_domain_health.json"
    monkeypatch.setattr(CloudMailService, "_health_store_path", staticmethod(lambda: health_path))
    CloudMailService._runtime_domain_block_until = {}
    CloudMailService._domain_rotation_offsets = {}
    CloudMailService._domain_inflight_allocations = {}

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
    CloudMailService._runtime_domain_block_until = {}
    CloudMailService._domain_rotation_offsets = {}
    CloudMailService._domain_inflight_allocations = {}

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
