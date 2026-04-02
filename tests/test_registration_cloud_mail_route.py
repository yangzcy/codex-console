import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from src.database import crud
from src.database.session import DatabaseSessionManager
from src.web.routes import registration as registration_routes


def test_get_available_email_services_includes_cloud_mail_entries():
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "route_cloud_mail.db"
        manager = DatabaseSessionManager(f"sqlite:///{db_path}")
        manager.create_tables()
        manager.migrate_tables()

        session = manager.SessionLocal()
        try:
            crud.create_email_service(
                session,
                name="CloudMail A",
                service_type="cloud_mail",
                config={
                    "base_url": "https://mail.example.com",
                    "admin_email": "admin@example.com",
                    "admin_password": "secret",
                    "domain": ["a.example.com", "b.example.com"],
                },
                enabled=True,
                priority=1,
            )
        finally:
            session.close()

        original_get_db = registration_routes.get_db
        registration_routes.get_db = lambda: manager.session_scope()
        try:
            result = asyncio.run(registration_routes.get_available_email_services())
        finally:
            registration_routes.get_db = original_get_db

        assert result["cloud_mail"]["available"] is True
        assert result["cloud_mail"]["count"] == 1
        assert result["cloud_mail"]["services"][0]["name"] == "CloudMail A"
        assert result["cloud_mail"]["services"][0]["domain"] == "a.example.com"
