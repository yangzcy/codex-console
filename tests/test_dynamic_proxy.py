from src.core import dynamic_proxy
from src.config import settings as settings_module


def test_report_dynamic_proxy_result_sets_local_cooldown(monkeypatch, tmp_path):
    health_path = tmp_path / "dynamic_proxy_health.json"
    monkeypatch.setattr(dynamic_proxy, "_health_store_path", lambda: health_path)

    ok = dynamic_proxy.report_dynamic_proxy_result(
        report_url="",
        proxy_url="http://127.0.0.1:41085",
        task_id="task-1",
        success=False,
        reason="register_create_account_retryable",
        detail="Failed to create account. Please try again.",
    )

    assert ok is False
    assert dynamic_proxy._is_proxy_cooling("http://127.0.0.1:41085") is True


def test_report_dynamic_proxy_result_success_clears_local_cooldown(monkeypatch, tmp_path):
    health_path = tmp_path / "dynamic_proxy_health.json"
    monkeypatch.setattr(dynamic_proxy, "_health_store_path", lambda: health_path)

    dynamic_proxy.report_dynamic_proxy_result(
        report_url="",
        proxy_url="http://127.0.0.1:41085",
        task_id="task-1",
        success=False,
        reason="register_create_account_retryable",
        detail="Failed to create account. Please try again.",
    )
    dynamic_proxy.report_dynamic_proxy_result(
        report_url="",
        proxy_url="http://127.0.0.1:41085",
        task_id="task-2",
        success=True,
        reason="success",
        detail="",
    )

    assert dynamic_proxy._is_proxy_cooling("http://127.0.0.1:41085") is False


def test_get_dynamic_proxy_for_task_skips_local_cooling_proxy(monkeypatch, tmp_path):
    health_path = tmp_path / "dynamic_proxy_health.json"
    monkeypatch.setattr(dynamic_proxy, "_health_store_path", lambda: health_path)

    dynamic_proxy.report_dynamic_proxy_result(
        report_url="",
        proxy_url="http://127.0.0.1:41085",
        task_id="task-1",
        success=False,
        reason="register_create_account_retryable",
        detail="Failed to create account. Please try again.",
    )

    class _Settings:
        proxy_dynamic_enabled = True
        proxy_dynamic_api_url = "https://proxy.example.com/api"
        proxy_dynamic_api_key = None
        proxy_dynamic_api_key_header = "X-API-Key"
        proxy_dynamic_result_field = ""

    calls = [
        ("http://127.0.0.1:41085", ""),
        ("http://127.0.0.1:41081", ""),
    ]

    monkeypatch.setattr(settings_module, "get_settings", lambda force_reload=True: _Settings())
    monkeypatch.setattr(dynamic_proxy, "fetch_dynamic_proxy_with_meta", lambda **kwargs: calls.pop(0))

    proxy_url, proxy_id = dynamic_proxy.get_dynamic_proxy_for_task()

    assert proxy_url == "http://127.0.0.1:41081"
    assert proxy_id == ""
