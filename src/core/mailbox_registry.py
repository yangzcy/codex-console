from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .utils import get_data_dir

logger = logging.getLogger(__name__)


class MailboxRegistry:
    """轻量邮箱治理注册表。

    仅用于记录邮箱生命周期和失败原因，不参与现有主业务决策，
    以尽量降低对注册链路和其他模块的影响。
    """

    _lock = threading.Lock()
    _file_name = "mailbox_registry.json"

    @classmethod
    def _registry_path(cls) -> Path:
        data_dir = get_data_dir()
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / cls._file_name

    @classmethod
    def _load(cls) -> Dict[str, Any]:
        path = cls._registry_path()
        if not path.exists():
            return {"mailboxes": {}}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("mailboxes"), dict):
                return data
        except Exception as exc:
            logger.warning("读取邮箱治理注册表失败: %s", exc)
        return {"mailboxes": {}}

    @classmethod
    def _save(cls, payload: Dict[str, Any]) -> None:
        path = cls._registry_path()
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temp_path.replace(path)

    @staticmethod
    def _utc_now() -> str:
        return datetime.utcnow().isoformat()

    @classmethod
    def _upsert(
        cls,
        *,
        email: str,
        service_type: Optional[str] = None,
        service_id: Optional[str] = None,
        task_uuid: Optional[str] = None,
        state: Optional[str] = None,
        reason_code: Optional[str] = None,
        increment_fail_count: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email:
            return None

        now = cls._utc_now()
        with cls._lock:
            payload = cls._load()
            mailboxes = payload.setdefault("mailboxes", {})
            record = mailboxes.get(normalized_email) or {
                "mailbox_email": normalized_email,
                "created_at": now,
                "fail_count": 0,
            }

            record["mailbox_email"] = normalized_email
            record["updated_at"] = now
            if service_type:
                record["service_type"] = str(service_type).strip()
            if service_id:
                record["service_id"] = str(service_id).strip()
            if task_uuid:
                record["last_task_uuid"] = str(task_uuid).strip()
            if state:
                record["state"] = str(state).strip()
            if reason_code:
                record["last_reason_code"] = str(reason_code).strip()
            if increment_fail_count:
                record["fail_count"] = int(record.get("fail_count") or 0) + 1
            if extra:
                for key, value in extra.items():
                    if value is not None:
                        record[key] = value

            mailboxes[normalized_email] = record
            cls._save(payload)
            return dict(record)

    @classmethod
    def mark_created(
        cls,
        *,
        email: str,
        service_type: str,
        service_id: Optional[str] = None,
        task_uuid: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return cls._upsert(
            email=email,
            service_type=service_type,
            service_id=service_id,
            task_uuid=task_uuid,
            state="in_use",
            extra={"last_used_at": cls._utc_now()},
        )

    @classmethod
    def mark_registered(
        cls,
        *,
        email: str,
        service_type: str,
        service_id: Optional[str] = None,
        task_uuid: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return cls._upsert(
            email=email,
            service_type=service_type,
            service_id=service_id,
            task_uuid=task_uuid,
            state="useful",
            extra={"registered_at": cls._utc_now(), "last_used_at": cls._utc_now()},
        )

    @classmethod
    def mark_registered_elsewhere(
        cls,
        *,
        email: str,
        service_type: str,
        service_id: Optional[str] = None,
        task_uuid: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return cls._upsert(
            email=email,
            service_type=service_type,
            service_id=service_id,
            task_uuid=task_uuid,
            state="registered_elsewhere",
            reason_code="email_already_registered_on_openai",
            increment_fail_count=True,
            extra={"quarantined_at": cls._utc_now()},
        )

    @classmethod
    def mark_deferred(
        cls,
        *,
        email: str,
        service_type: str,
        service_id: Optional[str] = None,
        task_uuid: Optional[str] = None,
        reason_code: Optional[str] = None,
        next_retry_at: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return cls._upsert(
            email=email,
            service_type=service_type,
            service_id=service_id,
            task_uuid=task_uuid,
            state="deferred_hold",
            reason_code=reason_code,
            increment_fail_count=True,
            extra={"next_retry_at": next_retry_at, "last_used_at": cls._utc_now()},
        )

    @classmethod
    def mark_failed(
        cls,
        *,
        email: str,
        service_type: str,
        service_id: Optional[str] = None,
        task_uuid: Optional[str] = None,
        reason_code: Optional[str] = None,
        hard_invalid: bool = False,
    ) -> Optional[Dict[str, Any]]:
        state = "invalid_hard" if hard_invalid else "suspect"
        extra: Dict[str, Any] = {"last_used_at": cls._utc_now()}
        if hard_invalid:
            extra["quarantined_at"] = cls._utc_now()
        return cls._upsert(
            email=email,
            service_type=service_type,
            service_id=service_id,
            task_uuid=task_uuid,
            state=state,
            reason_code=reason_code,
            increment_fail_count=True,
            extra=extra,
        )

    @classmethod
    def get(cls, email: str) -> Optional[Dict[str, Any]]:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email:
            return None
        with cls._lock:
            payload = cls._load()
            row = payload.get("mailboxes", {}).get(normalized_email)
            return dict(row) if isinstance(row, dict) else None
