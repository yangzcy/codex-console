"""
注册任务 API 路由
"""

import asyncio
import logging
import uuid
import random
from datetime import datetime
from typing import List, Optional, Dict, Tuple

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func

from ...config.constants import (
    RoleTag,
    normalize_role_tag,
    role_tag_to_account_label,
)
from ...database import crud
from ...database.session import get_db
from ...database.models import RegistrationTask, Proxy
from ...core.register import RegistrationEngine, RegistrationResult
from ...services import EmailServiceFactory, EmailServiceType
from ...config.settings import get_settings
from ..task_manager import task_manager

logger = logging.getLogger(__name__)
router = APIRouter()

BATCH_OTP_WAIT_SLICE_SECONDS = 15
BATCH_OTP_WAIT_RETRY_DELAY_SECONDS = 20
BATCH_OTP_WAIT_MAX_DEFERS = 6

# 任务存储（简单的内存存储，生产环境应使用 Redis）
running_tasks: dict = {}
# 批量任务存储
batch_tasks: Dict[str, dict] = {}


def _model_dump(data) -> dict:
    """兼容 Pydantic v1/v2 的导出方法"""
    if hasattr(data, "model_dump"):
        return data.model_dump()
    return data.dict()


def _load_batch_from_db(batch_id: str) -> Optional[dict]:
    """从数据库恢复批量任务状态"""
    with get_db() as db:
        batch = crud.get_registration_batch_by_batch_id(db, batch_id)
        if not batch:
            return None
        return {
            "batch_id": batch.batch_id,
            "total": batch.total or 0,
            "completed": batch.completed or 0,
            "success": batch.success or 0,
            "failed": batch.failed or 0,
            "skipped": batch.skipped or 0,
            "cancelled": bool(batch.cancelled),
            "current_index": batch.current_index or 0,
            "finished": bool(batch.finished),
            "status": batch.status or "pending",
            "error_message": batch.error_message,
            "logs": (batch.logs or "").splitlines() if batch.logs else [],
            "mode": batch.mode,
            "batch_type": batch.batch_type,
        }


def _get_batch_status_payload(batch_id: str) -> Optional[dict]:
    """获取批量任务状态，优先内存，回退数据库"""
    batch = batch_tasks.get(batch_id)
    if batch is None:
        batch = _load_batch_from_db(batch_id)
        if batch is None:
            return None

    total = batch.get("total", 0)
    completed = batch.get("completed", 0)
    return {
        "batch_id": batch_id,
        "total": total,
        "completed": completed,
        "success": batch.get("success", 0),
        "failed": batch.get("failed", 0),
        "skipped": batch.get("skipped", 0),
        "current_index": batch.get("current_index", 0),
        "cancelled": batch.get("cancelled", False),
        "finished": batch.get("finished", False),
        "status": batch.get("status", "pending"),
        "error_message": batch.get("error_message"),
        "logs": batch.get("logs", []),
        "progress": f"{completed}/{total}",
    }


def _persist_batch_update(batch_id: str, **kwargs) -> None:
    """将批量任务状态落库"""
    if kwargs.get("finished") and "completed_at" not in kwargs:
        kwargs["completed_at"] = datetime.utcnow()

    with get_db() as db:
        crud.update_registration_batch(
            db,
            batch_id,
            updated_at=datetime.utcnow(),
            **kwargs,
        )


def _persist_batch_log(batch_id: str, log_message: str) -> None:
    """将批量任务日志落库"""
    with get_db() as db:
        crud.append_registration_batch_log(db, batch_id, log_message)
        crud.update_registration_batch(db, batch_id, updated_at=datetime.utcnow())


def _request_batch_cancel(batch_id: str) -> bool:
    """标记批量任务及其子任务为取消中。"""
    payload = _get_batch_status_payload(batch_id)
    if payload is None:
        return False

    if batch_id in batch_tasks:
        batch_tasks[batch_id]["cancelled"] = True
        batch_tasks[batch_id]["status"] = "cancelling"

    task_manager.cancel_batch(batch_id)

    with get_db() as db:
        tasks = crud.get_registration_tasks_by_batch_id(db, batch_id)
        for task in tasks:
            task_manager.cancel_task(task.task_uuid)
            if task.status in {"pending", "running"}:
                crud.update_registration_task(
                    db,
                    task.task_uuid,
                    status="cancelled",
                    error_message="用户手动取消批量任务",
                    completed_at=datetime.utcnow(),
                )

    _persist_batch_update(
        batch_id,
        cancelled=True,
        status="cancelling",
        error_message="用户手动取消批量任务",
    )
    _persist_batch_log(batch_id, "[取消] 用户手动停止了批量任务")
    return True


def recover_interrupted_batches() -> None:
    """启动时收敛上次因重启中断的批量任务，避免长期卡在 pending。"""
    restart_reason = "服务在批量任务执行期间重启，本轮任务已中断，请重新发起。"

    with get_db() as db:
        unfinished_batches = crud.get_unfinished_registration_batches(db)
        for batch in unfinished_batches:
            if str(batch.status or "").lower() in {"completed", "failed", "cancelled"}:
                crud.update_registration_batch(
                    db,
                    batch.batch_id,
                    finished=True,
                    completed_at=batch.completed_at or datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                continue

            existing_logs = batch.logs or ""
            cancelled_marker = bool(batch.cancelled) or str(batch.status or "").lower() in {"cancelling", "cancelled"}
            final_status = "cancelled" if cancelled_marker else "failed"
            final_reason = "用户手动取消批量任务" if cancelled_marker else restart_reason
            recovery_log = f"[系统] {final_reason}"
            merged_logs = f"{existing_logs}\n{recovery_log}" if existing_logs else recovery_log
            crud.update_registration_batch(
                db,
                batch.batch_id,
                status=final_status,
                finished=True,
                cancelled=cancelled_marker,
                error_message=final_reason,
                logs=merged_logs,
                completed_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

            for task in crud.get_registration_tasks_by_batch_id(db, batch.batch_id):
                if task.status in {"pending", "running"}:
                    task_error = task.error_message or final_reason
                    crud.update_registration_task(
                        db,
                        task.task_uuid,
                        status="cancelled" if cancelled_marker else "failed",
                        error_message=task_error,
                        completed_at=datetime.utcnow(),
                    )

            logger.warning(f"批量任务启动恢复已收敛: {batch.batch_id} -> {final_status}")


# ============== Proxy Helper Functions ==============

def get_proxy_for_registration(db) -> Tuple[Optional[str], Optional[int]]:
    """
    获取用于注册的代理

    策略：
    1. 优先从代理列表中随机选择一个启用的代理
    2. 如果代理列表为空且启用了动态代理，调用动态代理 API 获取
    3. 否则使用系统设置中的静态默认代理

    Returns:
        Tuple[proxy_url, proxy_id]: 代理 URL 和代理 ID（如果来自代理列表）
    """
    # 先尝试从代理列表中获取
    proxy = crud.get_random_proxy(db)
    if proxy:
        return proxy.proxy_url, proxy.id

    # 代理列表为空，尝试动态代理或静态代理
    from ...core.dynamic_proxy import get_proxy_url_for_task
    proxy_url = get_proxy_url_for_task()
    if proxy_url:
        return proxy_url, None

    return None, None


def update_proxy_usage(db, proxy_id: Optional[int]):
    """更新代理的使用时间"""
    if proxy_id:
        crud.update_proxy_last_used(db, proxy_id)


# ============== Pydantic Models ==============

class RegistrationTaskCreate(BaseModel):
    """创建注册任务请求"""
    email_service_type: str = "tempmail"
    proxy: Optional[str] = None
    email_service_config: Optional[dict] = None
    email_service_id: Optional[int] = None
    auto_upload_cpa: bool = False
    cpa_service_ids: List[int] = []  # 指定 CPA 服务 ID 列表，空则取第一个启用的
    auto_upload_sub2api: bool = False
    sub2api_service_ids: List[int] = []  # 指定 Sub2API 服务 ID 列表
    auto_upload_tm: bool = False
    tm_service_ids: List[int] = []  # 指定 TM 服务 ID 列表
    registration_type: str = RoleTag.CHILD.value  # none / parent / child


class BatchRegistrationRequest(BaseModel):
    """批量注册请求"""
    count: int = 1
    email_service_type: str = "tempmail"
    proxy: Optional[str] = None
    email_service_config: Optional[dict] = None
    email_service_id: Optional[int] = None
    interval_min: int = 20
    interval_max: int = 60
    concurrency: int = 1
    mode: str = "pipeline"
    auto_upload_cpa: bool = False
    cpa_service_ids: List[int] = []
    auto_upload_sub2api: bool = False
    sub2api_service_ids: List[int] = []
    auto_upload_tm: bool = False
    tm_service_ids: List[int] = []
    registration_type: str = RoleTag.CHILD.value  # none / parent / child

    @field_validator("count", mode="before")
    @classmethod
    def validate_count(cls, value):
        if value is None or value == "":
            raise ValueError("注册数量不能为空")
        if isinstance(value, bool):
            raise ValueError("注册数量必须是正整数")
        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("注册数量不能为空")
            if not value.isdigit():
                raise ValueError("注册数量必须是正整数")
        elif not isinstance(value, int):
            raise ValueError("注册数量必须是正整数")

        count = int(value)
        if count < 1:
            raise ValueError("注册数量必须是正整数")
        return count


class RegistrationTaskResponse(BaseModel):
    """注册任务响应"""
    id: int
    task_uuid: str
    status: str
    email_service_id: Optional[int] = None
    email_service: Optional[str] = None
    email_service_name: Optional[str] = None
    email: Optional[str] = None
    proxy: Optional[str] = None
    logs: Optional[str] = None
    result: Optional[dict] = None
    error_message: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    class Config:
        from_attributes = True


class BatchRegistrationResponse(BaseModel):
    """批量注册响应"""
    batch_id: str
    count: int
    tasks: List[RegistrationTaskResponse]


class TaskListResponse(BaseModel):
    """任务列表响应"""
    total: int
    tasks: List[RegistrationTaskResponse]


# ============== Outlook 批量注册模型 ==============

class OutlookAccountForRegistration(BaseModel):
    """可用于注册的 Outlook 账户"""
    id: int                      # EmailService 表的 ID
    email: str
    name: str
    has_oauth: bool              # 是否有 OAuth 配置
    is_registered: bool          # 是否已注册
    registered_account_id: Optional[int] = None


class OutlookAccountsListResponse(BaseModel):
    """Outlook 账户列表响应"""
    total: int
    registered_count: int        # 已注册数量
    unregistered_count: int      # 未注册数量
    accounts: List[OutlookAccountForRegistration]


class OutlookBatchRegistrationRequest(BaseModel):
    """Outlook 批量注册请求"""
    service_ids: List[int]
    skip_registered: bool = True
    proxy: Optional[str] = None
    interval_min: int = 20
    interval_max: int = 60
    concurrency: int = 1
    mode: str = "pipeline"
    auto_upload_cpa: bool = False
    cpa_service_ids: List[int] = []
    auto_upload_sub2api: bool = False
    sub2api_service_ids: List[int] = []
    auto_upload_tm: bool = False
    tm_service_ids: List[int] = []
    registration_type: str = RoleTag.CHILD.value  # none / parent / child


class OutlookBatchRegistrationResponse(BaseModel):
    """Outlook 批量注册响应"""
    batch_id: str
    total: int                   # 总数
    skipped: int                 # 跳过数（已注册）
    to_register: int             # 待注册数
    service_ids: List[int]       # 实际要注册的服务 ID


# ============== Helper Functions ==============

def task_to_response(task: RegistrationTask, db=None) -> RegistrationTaskResponse:
    """转换任务模型为响应"""
    email_service, email_service_name = _resolve_task_email_service(task, db=db)
    email = _resolve_task_email(task)

    return RegistrationTaskResponse(
        id=task.id,
        task_uuid=task.task_uuid,
        status=task.status,
        email_service_id=task.email_service_id,
        email_service=email_service,
        email_service_name=email_service_name,
        email=email,
        proxy=task.proxy,
        logs=task.logs,
        result=task.result,
        error_message=task.error_message,
        created_at=task.created_at.isoformat() if task.created_at else None,
        started_at=task.started_at.isoformat() if task.started_at else None,
        completed_at=task.completed_at.isoformat() if task.completed_at else None,
    )


SERVICE_DISPLAY_NAMES = {
    "tempmail": "Tempmail.lol",
    "outlook": "Outlook",
    "moe_mail": "MoeMail",
    "temp_mail": "Temp-Mail（自部署）",
    "duck_mail": "DuckMail",
    "freemail": "Freemail",
    "imap_mail": "IMAP 邮箱",
    "cloud_mail": "CloudMail",
}


def _resolve_task_email_service(task: RegistrationTask, db=None) -> Tuple[Optional[str], Optional[str]]:
    """从任务结果或邮箱服务配置中解析服务类型和显示名。"""
    result = task.result or {}
    metadata = result.get("metadata") if isinstance(result, dict) else {}

    service_type = None
    if isinstance(metadata, dict):
        service_type = metadata.get("email_service")

    if not service_type and isinstance(result, dict):
        service_type = result.get("email_service")

    service_name = SERVICE_DISPLAY_NAMES.get(service_type) if service_type else None

    if task.email_service_id:
        from ...database.models import EmailService as EmailServiceModel
        if db is not None:
            service = db.query(EmailServiceModel).filter(EmailServiceModel.id == task.email_service_id).first()
            if service:
                if not service_type:
                    service_type = service.service_type
                service_name = service.name or SERVICE_DISPLAY_NAMES.get(service_type) or service_type
        else:
            with get_db() as local_db:
                service = local_db.query(EmailServiceModel).filter(EmailServiceModel.id == task.email_service_id).first()
                if service:
                    if not service_type:
                        service_type = service.service_type
                    service_name = service.name or SERVICE_DISPLAY_NAMES.get(service_type) or service_type

    if service_type and not service_name:
        service_name = SERVICE_DISPLAY_NAMES.get(service_type, service_type)

    return service_type, service_name


def _resolve_task_email(task: RegistrationTask) -> Optional[str]:
    """从任务结果中解析邮箱地址。"""
    result = task.result or {}
    if isinstance(result, dict) and result.get("email"):
        return result.get("email")
    return None


def _normalize_email_service_config(
    service_type: EmailServiceType,
    config: Optional[dict],
) -> dict:
    """按服务类型兼容旧字段名，避免不同服务的配置键互相污染。"""
    normalized = config.copy() if config else {}
    normalized.pop("proxy_url", None)

    if 'api_url' in normalized and 'base_url' not in normalized:
        normalized['base_url'] = normalized.pop('api_url')

    if service_type == EmailServiceType.MOE_MAIL:
        if 'domain' in normalized and 'default_domain' not in normalized:
            normalized['default_domain'] = normalized.pop('domain')
    elif service_type in (EmailServiceType.TEMP_MAIL, EmailServiceType.FREEMAIL):
        if 'default_domain' in normalized and 'domain' not in normalized:
            normalized['domain'] = normalized.pop('default_domain')
    elif service_type == EmailServiceType.DUCK_MAIL:
        if 'domain' in normalized and 'default_domain' not in normalized:
            normalized['default_domain'] = normalized.pop('domain')

    return normalized


def _make_task_execution_outcome(outcome: str, error: str = "", email: str = "") -> dict:
    return {
        "outcome": outcome,
        "error": error,
        "email": email,
    }


def _classify_proxy_report_reason(error_text: str, outcome: str) -> str:
    text = str(error_text or "").strip().lower()
    outcome_key = str(outcome or "").strip().lower()
    if outcome_key == "completed":
        return "success"
    if "http 429" in text or "rate limit exceeded" in text or "too many requests" in text:
        return "http_429"
    if "token exchange failed" in text:
        return "token_exchange_fail"
    if "处理 oauth 回调失败" in text or "oauth callback" in text:
        return "oauth_callback_fail"
    if "failed to connect to auth.openai.com" in text or "auth.openai.com port 443" in text:
        return "auth_openai_unreachable"
    if "timed out" in text or "timeout" in text or "curl: (7)" in text:
        return "connect_timeout"
    return "unknown_error"


def _report_dynamic_proxy_result_if_needed(
    *,
    dynamic_proxy_used: bool,
    proxy_url: Optional[str],
    task_uuid: str,
    outcome: str,
    error_message: str = "",
) -> None:
    if not dynamic_proxy_used:
        return
    runtime_proxy = str(proxy_url or "").strip()
    if not runtime_proxy:
        return

    try:
        from ...core.dynamic_proxy import report_dynamic_proxy_result

        settings = get_settings()
        report_url = str(getattr(settings, "proxy_dynamic_report_url", "") or "").strip()
        if not report_url:
            return

        api_key = settings.proxy_dynamic_api_key.get_secret_value() if settings.proxy_dynamic_api_key else ""
        reason = _classify_proxy_report_reason(error_message, outcome)
        report_dynamic_proxy_result(
            report_url=report_url,
            proxy_url=runtime_proxy,
            task_id=task_uuid,
            success=(str(outcome).lower() == "completed"),
            reason=reason,
            detail=str(error_message or "").strip(),
            purpose="openai_register",
            api_key=api_key,
            api_key_header=settings.proxy_dynamic_api_key_header,
        )
    except Exception as exc:
        logger.warning("任务 %s 动态代理结果上报失败: %s", task_uuid, exc)

def _run_sync_registration_task(task_uuid: str, email_service_type: str, proxy: Optional[str], email_service_config: Optional[dict], email_service_id: Optional[int] = None, log_prefix: str = "", batch_id: str = "", auto_upload_cpa: bool = False, cpa_service_ids: List[int] = None, auto_upload_sub2api: bool = False, sub2api_service_ids: List[int] = None, auto_upload_tm: bool = False, tm_service_ids: List[int] = None, batch_wait_slice_seconds: Optional[int] = None, registration_type: str = RoleTag.CHILD.value):
    """
    在线程池中执行的同步注册任务

    这个函数会被 run_in_executor 调用，运行在独立线程中
    """
    openai_proxy_url: Optional[str] = None
    dynamic_proxy_used = False
    with get_db() as db:
        try:
            # 检查是否已取消
            if task_manager.is_cancelled(task_uuid):
                logger.info(f"任务 {task_uuid} 已取消，跳过执行")
                crud.update_registration_task(
                    db, task_uuid,
                    status="cancelled",
                    completed_at=datetime.utcnow(),
                    error_message="任务已取消",
                )
                task_manager.update_status(task_uuid, "cancelled", error="任务已取消")
                return _make_task_execution_outcome("cancelled", error="任务已取消")

            # 更新任务状态为运行中
            task = crud.update_registration_task(
                db, task_uuid,
                status="running",
                started_at=datetime.utcnow()
            )

            if not task:
                logger.error(f"任务不存在: {task_uuid}")
                return _make_task_execution_outcome("failed", error="任务不存在")

            # 更新 TaskManager 状态
            task_manager.update_status(task_uuid, "running")

            # 确定使用的代理
            # 如果前端传入了代理参数，使用传入的
            # 否则从代理列表或系统设置中获取
            openai_proxy_url = proxy
            proxy_id = None

            if not openai_proxy_url:
                openai_proxy_url, proxy_id = get_proxy_for_registration(db)
                if openai_proxy_url:
                    logger.info(f"任务 {task_uuid} 使用 OpenAI 代理: {openai_proxy_url[:50]}...")
                    settings_snapshot = get_settings()
                    dynamic_proxy_used = (
                        proxy_id is None
                        and bool(getattr(settings_snapshot, "proxy_dynamic_enabled", False))
                        and bool(str(getattr(settings_snapshot, "proxy_dynamic_api_url", "") or "").strip())
                    )

            # 更新任务的代理记录
            crud.update_registration_task(db, task_uuid, proxy=openai_proxy_url)

            # 创建邮箱服务
            service_type = EmailServiceType(email_service_type)
            settings = get_settings()

            # 优先使用数据库中配置的邮箱服务
            if email_service_id:
                from ...database.models import EmailService as EmailServiceModel
                db_service = db.query(EmailServiceModel).filter(
                    EmailServiceModel.id == email_service_id,
                    EmailServiceModel.enabled == True
                ).first()

                if db_service:
                    service_type = EmailServiceType(db_service.service_type)
                    config = _normalize_email_service_config(service_type, db_service.config)
                    # 更新任务关联的邮箱服务
                    crud.update_registration_task(db, task_uuid, email_service_id=db_service.id)
                    logger.info(f"使用数据库邮箱服务: {db_service.name} (ID: {db_service.id}, 类型: {service_type.value})")
                else:
                    raise ValueError(f"邮箱服务不存在或已禁用: {email_service_id}")
            else:
                # 使用默认配置或传入的配置
                if service_type == EmailServiceType.TEMPMAIL:
                    config = {
                        "base_url": settings.tempmail_base_url,
                        "timeout": settings.tempmail_timeout,
                        "max_retries": settings.tempmail_max_retries,
                    }
                elif service_type == EmailServiceType.MOE_MAIL:
                    # 检查数据库中是否有可用的自定义域名服务
                    from ...database.models import EmailService as EmailServiceModel
                    db_service = db.query(EmailServiceModel).filter(
                        EmailServiceModel.service_type == "moe_mail",
                        EmailServiceModel.enabled == True
                    ).order_by(EmailServiceModel.priority.asc()).first()

                    if db_service and db_service.config:
                        config = _normalize_email_service_config(service_type, db_service.config)
                        crud.update_registration_task(db, task_uuid, email_service_id=db_service.id)
                        logger.info(f"使用数据库自定义域名服务: {db_service.name}")
                    elif settings.custom_domain_base_url and settings.custom_domain_api_key:
                        config = {
                            "base_url": settings.custom_domain_base_url,
                            "api_key": settings.custom_domain_api_key.get_secret_value() if settings.custom_domain_api_key else "",
                        }
                    else:
                        raise ValueError("没有可用的自定义域名邮箱服务，请先在设置中配置")
                elif service_type == EmailServiceType.OUTLOOK:
                    # 检查数据库中是否有可用的 Outlook 账户
                    from ...database.models import EmailService as EmailServiceModel, Account
                    # 获取所有启用的 Outlook 服务
                    outlook_services = db.query(EmailServiceModel).filter(
                        EmailServiceModel.service_type == "outlook",
                        EmailServiceModel.enabled == True
                    ).order_by(EmailServiceModel.priority.asc()).all()

                    if not outlook_services:
                        raise ValueError("没有可用的 Outlook 账户，请先在设置中导入账户")

                    # 找到一个未注册的 Outlook 账户
                    selected_service = None
                    for svc in outlook_services:
                        email = svc.config.get("email") if svc.config else None
                        if not email:
                            continue
                        normalized_email = str(email).strip().lower()
                        # 检查是否已在 accounts 表中注册
                        existing = db.query(Account).filter(
                            func.lower(Account.email) == normalized_email
                        ).first()
                        if not existing:
                            selected_service = svc
                            logger.info(f"选择未注册的 Outlook 账户: {email}")
                            break
                        else:
                            logger.info(f"跳过已注册的 Outlook 账户: {email}")

                    if selected_service and selected_service.config:
                        config = selected_service.config.copy()
                        crud.update_registration_task(db, task_uuid, email_service_id=selected_service.id)
                        logger.info(f"使用数据库 Outlook 账户: {selected_service.name}")
                    else:
                        raise ValueError("所有 Outlook 账户都已注册过 OpenAI 账号，请添加新的 Outlook 账户")
                elif service_type == EmailServiceType.DUCK_MAIL:
                    from ...database.models import EmailService as EmailServiceModel

                    db_service = db.query(EmailServiceModel).filter(
                        EmailServiceModel.service_type == "duck_mail",
                        EmailServiceModel.enabled == True
                    ).order_by(EmailServiceModel.priority.asc()).first()

                    if db_service and db_service.config:
                        config = _normalize_email_service_config(service_type, db_service.config)
                        crud.update_registration_task(db, task_uuid, email_service_id=db_service.id)
                        logger.info(f"使用数据库 DuckMail 服务: {db_service.name}")
                    else:
                        raise ValueError("没有可用的 DuckMail 邮箱服务，请先在邮箱服务页面添加服务")
                elif service_type == EmailServiceType.FREEMAIL:
                    from ...database.models import EmailService as EmailServiceModel

                    db_service = db.query(EmailServiceModel).filter(
                        EmailServiceModel.service_type == "freemail",
                        EmailServiceModel.enabled == True
                    ).order_by(EmailServiceModel.priority.asc()).first()

                    if db_service and db_service.config:
                        config = _normalize_email_service_config(service_type, db_service.config)
                        crud.update_registration_task(db, task_uuid, email_service_id=db_service.id)
                        logger.info(f"使用数据库 Freemail 服务: {db_service.name}")
                    else:
                        raise ValueError("没有可用的 Freemail 邮箱服务，请先在邮箱服务页面添加服务")
                elif service_type == EmailServiceType.IMAP_MAIL:
                    from ...database.models import EmailService as EmailServiceModel

                    db_service = db.query(EmailServiceModel).filter(
                        EmailServiceModel.service_type == "imap_mail",
                        EmailServiceModel.enabled == True
                    ).order_by(EmailServiceModel.priority.asc()).first()

                    if db_service and db_service.config:
                        config = _normalize_email_service_config(service_type, db_service.config)
                        crud.update_registration_task(db, task_uuid, email_service_id=db_service.id)
                        logger.info(f"使用数据库 IMAP 邮箱服务: {db_service.name}")
                    else:
                        raise ValueError("没有可用的 IMAP 邮箱服务，请先在邮箱服务中添加")
                else:
                    config = _normalize_email_service_config(service_type, email_service_config)

            email_service = EmailServiceFactory.create(service_type, config)

            # 创建注册引擎 - 使用 TaskManager 的日志回调
            log_callback = task_manager.create_log_callback(task_uuid, prefix=log_prefix, batch_id=batch_id)

            engine = RegistrationEngine(
                email_service=email_service,
                proxy_url=openai_proxy_url,
                callback_logger=log_callback,
                task_uuid=task_uuid,
                batch_wait_slice_seconds=batch_wait_slice_seconds,
                check_cancelled=task_manager.create_check_cancelled_callback(task_uuid),
            )

            # 执行注册
            role_tag = normalize_role_tag(registration_type)
            account_label = role_tag_to_account_label(role_tag)
            result = engine.run()

            if task_manager.is_cancelled(task_uuid) and not result.success:
                reason = "任务已取消"
                crud.update_registration_task(
                    db, task_uuid,
                    status="cancelled",
                    completed_at=datetime.utcnow(),
                    error_message=reason,
                )
                task_manager.update_status(task_uuid, "cancelled", error=reason)
                logger.info(f"任务 {task_uuid} 在执行后检测到取消请求，按取消收尾")
                return _make_task_execution_outcome("cancelled", error=reason)

            if result.success:
                # 更新代理使用时间
                update_proxy_usage(db, proxy_id)

                metadata = result.metadata if isinstance(result.metadata, dict) else {}
                metadata["account_label"] = account_label
                metadata["role_tag"] = role_tag
                metadata["registration_type"] = role_tag
                result.metadata = metadata

                # 保存到数据库
                engine.save_to_database(result, account_label=account_label, role_tag=role_tag)

                # 自动上传到 CPA（可多服务）
                if auto_upload_cpa:
                    try:
                        from ...core.upload.cpa_upload import upload_to_cpa, generate_token_json
                        from ...database.models import Account as AccountModel
                        saved_account = db.query(AccountModel).filter_by(email=result.email).first()
                        if saved_account and saved_account.access_token:
                            token_data = generate_token_json(saved_account)
                            _cpa_ids = cpa_service_ids or []
                            if not _cpa_ids:
                                # 未指定则取所有启用的服务
                                _cpa_ids = [s.id for s in crud.get_cpa_services(db, enabled=True)]
                            if not _cpa_ids:
                                log_callback("[CPA] 无可用 CPA 服务，跳过上传")
                            for _sid in _cpa_ids:
                                try:
                                    _svc = crud.get_cpa_service_by_id(db, _sid)
                                    if not _svc:
                                        continue
                                    log_callback(f"[CPA] 正在把账号打包发往服务站: {_svc.name}")
                                    _ok, _msg = upload_to_cpa(token_data, api_url=_svc.api_url, api_token=_svc.api_token)
                                    if _ok:
                                        saved_account.cpa_uploaded = True
                                        saved_account.cpa_uploaded_at = datetime.utcnow()
                                        db.commit()
                                        log_callback(f"[CPA] 投递成功，服务站已签收: {_svc.name}")
                                    else:
                                        log_callback(f"[CPA] 上传失败({_svc.name}): {_msg}")
                                except Exception as _e:
                                    log_callback(f"[CPA] 异常({_sid}): {_e}")
                    except Exception as cpa_err:
                        log_callback(f"[CPA] 上传异常: {cpa_err}")

                # 自动上传到 Sub2API（可多服务）
                if auto_upload_sub2api:
                    try:
                        from ...core.upload.sub2api_upload import upload_to_sub2api
                        from ...database.models import Account as AccountModel
                        saved_account = db.query(AccountModel).filter_by(email=result.email).first()
                        if saved_account and saved_account.access_token:
                            _s2a_ids = sub2api_service_ids or []
                            if not _s2a_ids:
                                _s2a_ids = [s.id for s in crud.get_sub2api_services(db, enabled=True)]
                            if not _s2a_ids:
                                log_callback("[Sub2API] 无可用 Sub2API 服务，跳过上传")
                            for _sid in _s2a_ids:
                                try:
                                    _svc = crud.get_sub2api_service_by_id(db, _sid)
                                    if not _svc:
                                        continue
                                    log_callback(f"[Sub2API] 正在把账号发往服务站: {_svc.name}")
                                    _ok, _msg = upload_to_sub2api([saved_account], _svc.api_url, _svc.api_key)
                                    log_callback(f"[Sub2API] {'成功' if _ok else '失败'}({_svc.name}): {_msg}")
                                except Exception as _e:
                                    log_callback(f"[Sub2API] 异常({_sid}): {_e}")
                    except Exception as s2a_err:
                        log_callback(f"[Sub2API] 上传异常: {s2a_err}")

                # 自动上传到 Team Manager（可多服务）
                if auto_upload_tm:
                    try:
                        from ...core.upload.team_manager_upload import upload_to_team_manager
                        from ...database.models import Account as AccountModel
                        saved_account = db.query(AccountModel).filter_by(email=result.email).first()
                        if saved_account and saved_account.access_token:
                            _tm_ids = tm_service_ids or []
                            if not _tm_ids:
                                _tm_ids = [s.id for s in crud.get_tm_services(db, enabled=True)]
                            if not _tm_ids:
                                log_callback("[TM] 无可用 Team Manager 服务，跳过上传")
                            for _sid in _tm_ids:
                                try:
                                    _svc = crud.get_tm_service_by_id(db, _sid)
                                    if not _svc:
                                        continue
                                    log_callback(f"[TM] 正在把账号发往服务站: {_svc.name}")
                                    _ok, _msg = upload_to_team_manager(saved_account, _svc.api_url, _svc.api_key)
                                    log_callback(f"[TM] {'成功' if _ok else '失败'}({_svc.name}): {_msg}")
                                except Exception as _e:
                                    log_callback(f"[TM] 异常({_sid}): {_e}")
                    except Exception as tm_err:
                        log_callback(f"[TM] 上传异常: {tm_err}")

                # 更新任务状态
                crud.update_registration_task(
                    db, task_uuid,
                    status="completed",
                    completed_at=datetime.utcnow(),
                    result=result.to_dict(),
                    error_message=None,
                )

                # 更新 TaskManager 状态
                task_manager.update_status(task_uuid, "completed", email=result.email)
                _report_dynamic_proxy_result_if_needed(
                    dynamic_proxy_used=dynamic_proxy_used,
                    proxy_url=openai_proxy_url,
                    task_uuid=task_uuid,
                    outcome="completed",
                    error_message="",
                )

                logger.info(f"注册任务完成: {task_uuid}, 邮箱: {result.email}")
                return _make_task_execution_outcome("completed", email=result.email)
            elif result.metadata and result.metadata.get("batch_wait_deferred"):
                reason = str(result.metadata.get("batch_wait_defer_reason") or result.error_message or "验证码暂未到达，稍后继续")
                crud.update_registration_task(
                    db, task_uuid,
                    status="pending",
                    started_at=None,
                    completed_at=None,
                    error_message=reason,
                )
                task_manager.update_status(task_uuid, "pending", deferred=True, reason=reason)
                logger.info(f"注册任务分段让出并发位: {task_uuid}, 原因: {reason}")
                return _make_task_execution_outcome("deferred", error=reason)
            else:
                # 更新任务状态为失败
                if task_manager.is_cancelled(task_uuid):
                    reason = "任务已取消"
                    crud.update_registration_task(
                        db, task_uuid,
                        status="cancelled",
                        completed_at=datetime.utcnow(),
                        error_message=reason
                    )
                    task_manager.update_status(task_uuid, "cancelled", error=reason)
                    logger.info(f"注册任务取消收尾: {task_uuid}")
                    return _make_task_execution_outcome("cancelled", error=reason)

                crud.update_registration_task(
                    db, task_uuid,
                    status="failed",
                    completed_at=datetime.utcnow(),
                    error_message=result.error_message
                )

                # 更新 TaskManager 状态
                task_manager.update_status(task_uuid, "failed", error=result.error_message)
                _report_dynamic_proxy_result_if_needed(
                    dynamic_proxy_used=dynamic_proxy_used,
                    proxy_url=openai_proxy_url,
                    task_uuid=task_uuid,
                    outcome="failed",
                    error_message=result.error_message,
                )

                logger.warning(f"注册任务失败: {task_uuid}, 原因: {result.error_message}")
                return _make_task_execution_outcome("failed", error=result.error_message)

        except Exception as e:
            logger.error(f"注册任务异常: {task_uuid}, 错误: {e}")

            try:
                with get_db() as db:
                    if task_manager.is_cancelled(task_uuid):
                        crud.update_registration_task(
                            db, task_uuid,
                            status="cancelled",
                            completed_at=datetime.utcnow(),
                            error_message="任务已取消"
                        )
                        task_manager.update_status(task_uuid, "cancelled", error="任务已取消")
                        return _make_task_execution_outcome("cancelled", error="任务已取消")

                    crud.update_registration_task(
                        db, task_uuid,
                        status="failed",
                        completed_at=datetime.utcnow(),
                        error_message=str(e)
                    )

                # 更新 TaskManager 状态
                task_manager.update_status(task_uuid, "failed", error=str(e))
                _report_dynamic_proxy_result_if_needed(
                    dynamic_proxy_used=dynamic_proxy_used,
                    proxy_url=openai_proxy_url,
                    task_uuid=task_uuid,
                    outcome="failed",
                    error_message=str(e),
                )
            except:
                pass
            return _make_task_execution_outcome("failed", error=str(e))


async def run_registration_task(task_uuid: str, email_service_type: str, proxy: Optional[str], email_service_config: Optional[dict], email_service_id: Optional[int] = None, log_prefix: str = "", batch_id: str = "", auto_upload_cpa: bool = False, cpa_service_ids: List[int] = None, auto_upload_sub2api: bool = False, sub2api_service_ids: List[int] = None, auto_upload_tm: bool = False, tm_service_ids: List[int] = None, batch_wait_slice_seconds: Optional[int] = None, registration_type: str = RoleTag.CHILD.value):
    """
    异步执行注册任务

    使用 run_in_executor 将同步任务放入线程池执行，避免阻塞主事件循环
    """
    loop = task_manager.get_loop()
    if loop is None:
        loop = asyncio.get_event_loop()
        task_manager.set_loop(loop)

    # 初始化 TaskManager 状态
    task_manager.update_status(task_uuid, "pending")
    task_manager.add_log(task_uuid, f"{log_prefix} [系统] 任务 {task_uuid[:8]} 已加入队列" if log_prefix else f"[系统] 任务 {task_uuid[:8]} 已加入队列")

    try:
        # 在线程池中执行同步任务（传入 log_prefix 和 batch_id 供回调使用）
        return await loop.run_in_executor(
            task_manager.executor,
            _run_sync_registration_task,
            task_uuid,
            email_service_type,
            proxy,
            email_service_config,
            email_service_id,
            log_prefix,
            batch_id,
            auto_upload_cpa,
            cpa_service_ids or [],
            auto_upload_sub2api,
            sub2api_service_ids or [],
            auto_upload_tm,
            tm_service_ids or [],
            batch_wait_slice_seconds,
            registration_type,
        )
    except Exception as e:
        logger.error(f"线程池执行异常: {task_uuid}, 错误: {e}")
        task_manager.add_log(task_uuid, f"[错误] 线程池执行异常: {str(e)}")
        task_manager.update_status(task_uuid, "failed", error=str(e))
        return _make_task_execution_outcome("failed", error=str(e))


def _init_batch_state(batch_id: str, task_uuids: List[str]):
    """初始化批量任务内存状态"""
    import time
    previous = batch_tasks.get(batch_id, {})
    task_manager.init_batch(batch_id, len(task_uuids))
    batch_tasks[batch_id] = {
        "batch_id": batch_id,
        "total": len(task_uuids),
        "completed": 0,
        "success": 0,
        "failed": 0,
        "skipped": previous.get("skipped", 0),
        "status": "running",
        "error_message": None,
        "cancelled": False,
        "task_uuids": task_uuids,
        "service_ids": previous.get("service_ids", []),
        "current_index": 0,
        "logs": [],
        "finished": False,
        "start_time": time.time(),  # 记录开始时间
    }
    _persist_batch_update(
        batch_id,
        status="running",
        finished=False,
        cancelled=False,
        completed=0,
        success=0,
        failed=0,
        skipped=previous.get("skipped", 0),
        current_index=0,
        error_message=None,
        started_at=datetime.utcnow(),
        completed_at=None,
    )


def _make_batch_helpers(batch_id: str):
    """返回 add_batch_log 和 update_batch_status 辅助函数"""
    def add_batch_log(msg: str):
        batch_tasks[batch_id]["logs"].append(msg)
        task_manager.add_batch_log(batch_id, msg)
        _persist_batch_log(batch_id, msg)

    def update_batch_status(**kwargs):
        for key, value in kwargs.items():
            if key in batch_tasks[batch_id]:
                batch_tasks[batch_id][key] = value
        task_manager.update_batch_status(batch_id, **kwargs)
        _persist_batch_update(batch_id, **kwargs)

    return add_batch_log, update_batch_status
async def run_batch_parallel(
    batch_id: str,
    task_uuids: List[str],
    email_service_type: str,
    proxy: Optional[str],
    email_service_config: Optional[dict],
    email_service_id: Optional[int],
    concurrency: int,
    auto_upload_cpa: bool = False,
    cpa_service_ids: List[int] = None,
    auto_upload_sub2api: bool = False,
    sub2api_service_ids: List[int] = None,
    auto_upload_tm: bool = False,
    tm_service_ids: List[int] = None,
    registration_type: str = RoleTag.CHILD.value,
):
    """
    并行模式：所有任务同时提交，Semaphore 控制最大并发数
    """
    _init_batch_state(batch_id, task_uuids)
    add_batch_log, update_batch_status = _make_batch_helpers(batch_id)
    semaphore = asyncio.Semaphore(concurrency)
    counter_lock = asyncio.Lock()
    add_batch_log(f"[系统] 并行模式启动，并发数: {concurrency}，总任务: {len(task_uuids)}")

    async def _run_one(idx: int, uuid: str):
        prefix = f"[任务{idx + 1}]"
        defer_count = 0
        while True:
            async with semaphore:
                outcome = await run_registration_task(
                    uuid, email_service_type, proxy, email_service_config, email_service_id,
                    log_prefix=prefix, batch_id=batch_id,
                    auto_upload_cpa=auto_upload_cpa, cpa_service_ids=cpa_service_ids or [],
                    auto_upload_sub2api=auto_upload_sub2api, sub2api_service_ids=sub2api_service_ids or [],
                    auto_upload_tm=auto_upload_tm, tm_service_ids=tm_service_ids or [],
                    batch_wait_slice_seconds=BATCH_OTP_WAIT_SLICE_SECONDS,
                registration_type=registration_type,
            )

            status = str((outcome or {}).get("outcome") or "")
            if status == "deferred" and defer_count < BATCH_OTP_WAIT_MAX_DEFERS and not task_manager.is_batch_cancelled(batch_id):
                defer_count += 1
                add_batch_log(
                    f"{prefix} [让出] {str((outcome or {}).get('error') or '验证码暂未到达')}，"
                    f"{BATCH_OTP_WAIT_RETRY_DELAY_SECONDS}s 后继续第 {defer_count}/{BATCH_OTP_WAIT_MAX_DEFERS} 轮"
                )
                await asyncio.sleep(BATCH_OTP_WAIT_RETRY_DELAY_SECONDS)
                continue

            if status == "deferred":
                outcome = {
                    "outcome": "failed",
                    "error": f"验证码等待片段已用尽（{BATCH_OTP_WAIT_MAX_DEFERS} 轮），停止继续排队",
                    "email": "",
                }

            with get_db() as db:
                t = crud.get_registration_task(db, uuid)
                if t:
                    async with counter_lock:
                        new_completed = batch_tasks[batch_id]["completed"] + 1
                        new_success = batch_tasks[batch_id]["success"]
                        new_failed = batch_tasks[batch_id]["failed"]
                        if str((outcome or {}).get("outcome") or "") == "completed" or t.status == "completed":
                            new_success += 1
                            add_batch_log(f"{prefix} [成功] 注册成功")
                        else:
                            new_failed += 1
                            add_batch_log(f"{prefix} [失败] 注册失败: {str((outcome or {}).get('error') or t.error_message or '未知错误')}")
                        update_batch_status(completed=new_completed, success=new_success, failed=new_failed)
                break

    try:
        import time
        start_time = time.time()  # 记录开始时间
        
        await asyncio.gather(*[_run_one(i, u) for i, u in enumerate(task_uuids)], return_exceptions=True)
        
        # 计算总耗时
        end_time = time.time()
        total_seconds = end_time - start_time
        
        if not task_manager.is_batch_cancelled(batch_id):
            success_count = batch_tasks[batch_id]['success']
            failed_count = batch_tasks[batch_id]['failed']
            
            # 计算平均每个账号的时间
            total_accounts = success_count + failed_count
            avg_time = total_seconds / total_accounts if total_accounts > 0 else 0
            
            # 格式化时间显示
            minutes = int(total_seconds // 60)
            seconds = int(total_seconds % 60)
            time_str = f"{minutes}分{seconds}秒" if minutes > 0 else f"{seconds}秒"
            
            if failed_count > 0:
                add_batch_log(f"[完成] 批量任务完成！成功: {success_count}, 未成功: {failed_count}")
            else:
                add_batch_log(f"[完成] 批量任务完成！✅ 全部成功: {success_count} 个")
            
            add_batch_log(f"[统计] 总耗时: {time_str}, 平均每个账号: {avg_time:.1f}秒")
            update_batch_status(finished=True, status="completed", error_message=None)
        else:
            update_batch_status(finished=True, status="cancelled", error_message="批量任务已取消")
    except Exception as e:
        logger.error(f"批量任务 {batch_id} 异常: {e}")
        add_batch_log(f"[错误] 批量任务异常: {str(e)}")
        update_batch_status(finished=True, status="failed", error_message=str(e))
    finally:
        batch_tasks[batch_id]["finished"] = True


async def run_batch_pipeline(
    batch_id: str,
    task_uuids: List[str],
    email_service_type: str,
    proxy: Optional[str],
    email_service_config: Optional[dict],
    email_service_id: Optional[int],
    interval_min: int,
    interval_max: int,
    concurrency: int,
    auto_upload_cpa: bool = False,
    cpa_service_ids: List[int] = None,
    auto_upload_sub2api: bool = False,
    sub2api_service_ids: List[int] = None,
    auto_upload_tm: bool = False,
    tm_service_ids: List[int] = None,
    registration_type: str = RoleTag.CHILD.value,
):
    """
    流水线模式：每隔 interval 秒启动一个新任务，Semaphore 限制最大并发数
    """
    _init_batch_state(batch_id, task_uuids)
    add_batch_log, update_batch_status = _make_batch_helpers(batch_id)
    semaphore = asyncio.Semaphore(concurrency)
    counter_lock = asyncio.Lock()
    running_tasks_list = []
    add_batch_log(f"[系统] 流水线模式启动，并发数: {concurrency}，总任务: {len(task_uuids)}")

    async def _run_and_release(idx: int, uuid: str, pfx: str):
        try:
            defer_count = 0
            while True:
                async with semaphore:
                    outcome = await run_registration_task(
                        uuid, email_service_type, proxy, email_service_config, email_service_id,
                        log_prefix=pfx, batch_id=batch_id,
                        auto_upload_cpa=auto_upload_cpa, cpa_service_ids=cpa_service_ids or [],
                        auto_upload_sub2api=auto_upload_sub2api, sub2api_service_ids=sub2api_service_ids or [],
                        auto_upload_tm=auto_upload_tm, tm_service_ids=tm_service_ids or [],
                        batch_wait_slice_seconds=BATCH_OTP_WAIT_SLICE_SECONDS,
                registration_type=registration_type,
            )

                status = str((outcome or {}).get("outcome") or "")
                if status == "deferred" and defer_count < BATCH_OTP_WAIT_MAX_DEFERS and not task_manager.is_batch_cancelled(batch_id):
                    defer_count += 1
                    add_batch_log(
                        f"{pfx} [让出] {str((outcome or {}).get('error') or '验证码暂未到达')}，"
                        f"{BATCH_OTP_WAIT_RETRY_DELAY_SECONDS}s 后继续第 {defer_count}/{BATCH_OTP_WAIT_MAX_DEFERS} 轮"
                    )
                    await asyncio.sleep(BATCH_OTP_WAIT_RETRY_DELAY_SECONDS)
                    continue

                if status == "deferred":
                    outcome = {
                        "outcome": "failed",
                        "error": f"验证码等待片段已用尽（{BATCH_OTP_WAIT_MAX_DEFERS} 轮），停止继续排队",
                        "email": "",
                    }

                with get_db() as db:
                    t = crud.get_registration_task(db, uuid)
                    if t:
                        async with counter_lock:
                            new_completed = batch_tasks[batch_id]["completed"] + 1
                            new_success = batch_tasks[batch_id]["success"]
                            new_failed = batch_tasks[batch_id]["failed"]
                            if str((outcome or {}).get("outcome") or "") == "completed" or t.status == "completed":
                                new_success += 1
                                add_batch_log(f"{pfx} [成功] 注册成功")
                            else:
                                new_failed += 1
                                add_batch_log(f"{pfx} [失败] 注册失败: {str((outcome or {}).get('error') or t.error_message or '未知错误')}")
                            update_batch_status(completed=new_completed, success=new_success, failed=new_failed)
                break
        finally:
            pass

    try:
        import time
        start_time = time.time()  # 记录开始时间
        
        for i, task_uuid in enumerate(task_uuids):
            if task_manager.is_batch_cancelled(batch_id) or batch_tasks[batch_id]["cancelled"]:
                with get_db() as db:
                    for remaining_uuid in task_uuids[i:]:
                        crud.update_registration_task(db, remaining_uuid, status="cancelled")
                add_batch_log("[取消] 批量任务已取消")
                update_batch_status(finished=True, status="cancelled", error_message="批量任务已取消")
                break

            update_batch_status(current_index=i)
            prefix = f"[任务{i + 1}]"
            add_batch_log(f"{prefix} 开始注册...")
            t = asyncio.create_task(_run_and_release(i, task_uuid, prefix))
            running_tasks_list.append(t)

            if i < len(task_uuids) - 1 and not task_manager.is_batch_cancelled(batch_id):
                wait_time = random.randint(interval_min, interval_max)
                logger.info(f"批量任务 {batch_id}: 等待 {wait_time} 秒后启动下一个任务")
                await asyncio.sleep(wait_time)

        if running_tasks_list:
            await asyncio.gather(*running_tasks_list, return_exceptions=True)

        # 计算总耗时
        end_time = time.time()
        total_seconds = end_time - start_time

        if not task_manager.is_batch_cancelled(batch_id):
            success_count = batch_tasks[batch_id]['success']
            failed_count = batch_tasks[batch_id]['failed']
            
            # 计算平均每个账号的时间
            total_accounts = success_count + failed_count
            avg_time = total_seconds / total_accounts if total_accounts > 0 else 0
            
            # 格式化时间显示
            minutes = int(total_seconds // 60)
            seconds = int(total_seconds % 60)
            time_str = f"{minutes}分{seconds}秒" if minutes > 0 else f"{seconds}秒"
            
            if failed_count > 0:
                add_batch_log(f"[完成] 批量任务完成！成功: {success_count}, 未成功: {failed_count}")
            else:
                add_batch_log(f"[完成] 批量任务完成！✅ 全部成功: {success_count} 个")
            
            add_batch_log(f"[统计] 总耗时: {time_str}, 平均每个账号: {avg_time:.1f}秒")
            update_batch_status(finished=True, status="completed", error_message=None)
    except Exception as e:
        logger.error(f"批量任务 {batch_id} 异常: {e}")
        add_batch_log(f"[错误] 批量任务异常: {str(e)}")
        update_batch_status(finished=True, status="failed", error_message=str(e))
    finally:
        batch_tasks[batch_id]["finished"] = True


async def run_batch_registration(
    batch_id: str,
    task_uuids: List[str],
    email_service_type: str,
    proxy: Optional[str],
    email_service_config: Optional[dict],
    email_service_id: Optional[int],
    interval_min: int,
    interval_max: int,
    concurrency: int = 1,
    mode: str = "pipeline",
    auto_upload_cpa: bool = False,
    cpa_service_ids: List[int] = None,
    auto_upload_sub2api: bool = False,
    sub2api_service_ids: List[int] = None,
    auto_upload_tm: bool = False,
    tm_service_ids: List[int] = None,
    registration_type: str = RoleTag.CHILD.value,
):
    """根据 mode 分发到并行或流水线执行"""
    if mode == "parallel":
        await run_batch_parallel(
            batch_id, task_uuids, email_service_type, proxy,
            email_service_config, email_service_id, concurrency,
            auto_upload_cpa=auto_upload_cpa, cpa_service_ids=cpa_service_ids,
            auto_upload_sub2api=auto_upload_sub2api, sub2api_service_ids=sub2api_service_ids,
            auto_upload_tm=auto_upload_tm, tm_service_ids=tm_service_ids,
            registration_type=registration_type,
        )
    else:
        await run_batch_pipeline(
            batch_id, task_uuids, email_service_type, proxy,
            email_service_config, email_service_id,
            interval_min, interval_max, concurrency,
            auto_upload_cpa=auto_upload_cpa, cpa_service_ids=cpa_service_ids,
            auto_upload_sub2api=auto_upload_sub2api, sub2api_service_ids=sub2api_service_ids,
            auto_upload_tm=auto_upload_tm, tm_service_ids=tm_service_ids,
            registration_type=registration_type,
        )


# ============== API Endpoints ==============

@router.post("/start", response_model=RegistrationTaskResponse)
async def start_registration(
    request: RegistrationTaskCreate,
    background_tasks: BackgroundTasks
):
    """
    启动注册任务

    - email_service_type: 邮箱服务类型 (tempmail, outlook, moe_mail)
    - proxy: 代理地址
    - email_service_config: 邮箱服务配置（outlook 需要提供账户信息）
    """
    # 验证邮箱服务类型
    try:
        EmailServiceType(request.email_service_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"无效的邮箱服务类型: {request.email_service_type}"
        )

    # 创建任务
    task_uuid = str(uuid.uuid4())

    with get_db() as db:
        task = crud.create_registration_task(
            db,
            task_uuid=task_uuid,
            proxy=request.proxy
        )

    # 在后台运行注册任务
    background_tasks.add_task(
        run_registration_task,
        task_uuid,
        request.email_service_type,
        request.proxy,
        request.email_service_config,
        request.email_service_id,
        "",
        "",
        request.auto_upload_cpa,
        request.cpa_service_ids,
        request.auto_upload_sub2api,
        request.sub2api_service_ids,
        request.auto_upload_tm,
        request.tm_service_ids,
        None,
        request.registration_type,
    )

    return task_to_response(task)


@router.post("/batch", response_model=BatchRegistrationResponse)
async def start_batch_registration(
    request: BatchRegistrationRequest,
    background_tasks: BackgroundTasks
):
    """
    启动批量注册任务

    - count: 注册数量 (正整数，当前上限 1000)
    - email_service_type: 邮箱服务类型
    - proxy: 代理地址
    - interval_min: 最小间隔秒数
    - interval_max: 最大间隔秒数
    """
    # 验证参数
    if request.count < 1 or request.count > 1000:
        raise HTTPException(status_code=400, detail="注册数量必须在 1-1000 之间")
    try:
        EmailServiceType(request.email_service_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"无效的邮箱服务类型: {request.email_service_type}"
        )

    if request.interval_min < 0 or request.interval_max < request.interval_min:
        raise HTTPException(status_code=400, detail="间隔时间参数无效")

    if not 1 <= request.concurrency <= 50:
        raise HTTPException(status_code=400, detail="并发数必须在 1-50 之间")

    if request.mode not in ("parallel", "pipeline"):
        raise HTTPException(status_code=400, detail="模式必须为 parallel 或 pipeline")

    # 创建批量任务
    batch_id = str(uuid.uuid4())
    task_uuids = []

    with get_db() as db:
        crud.create_registration_batch(
            db,
            batch_id=batch_id,
            batch_type="standard",
            mode=request.mode,
            total=request.count,
            email_service_type=request.email_service_type,
            email_service_id=request.email_service_id,
            proxy=request.proxy,
            interval_min=request.interval_min,
            interval_max=request.interval_max,
            concurrency=request.concurrency,
            request_payload=_model_dump(request),
        )
        for _ in range(request.count):
            task_uuid = str(uuid.uuid4())
            task = crud.create_registration_task(
                db,
                task_uuid=task_uuid,
                proxy=request.proxy,
                batch_id=batch_id,
            )
            task_uuids.append(task_uuid)

    # 获取所有任务
    with get_db() as db:
        tasks = [crud.get_registration_task(db, uuid) for uuid in task_uuids]

    # 在后台运行批量注册
    background_tasks.add_task(
        run_batch_registration,
        batch_id,
        task_uuids,
        request.email_service_type,
        request.proxy,
        request.email_service_config,
        request.email_service_id,
        request.interval_min,
        request.interval_max,
        request.concurrency,
        request.mode,
        request.auto_upload_cpa,
        request.cpa_service_ids,
        request.auto_upload_sub2api,
        request.sub2api_service_ids,
        request.auto_upload_tm,
        request.tm_service_ids,
        request.registration_type,
    )

    return BatchRegistrationResponse(
        batch_id=batch_id,
        count=request.count,
        tasks=[task_to_response(t) for t in tasks if t]
    )


@router.get("/batch/{batch_id}")
async def get_batch_status(batch_id: str):
    """获取批量任务状态"""
    payload = _get_batch_status_payload(batch_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="批量任务不存在")
    return payload


@router.post("/batch/{batch_id}/cancel")
async def cancel_batch(batch_id: str):
    """取消批量任务"""
    payload = _get_batch_status_payload(batch_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="批量任务不存在")

    if payload.get("finished"):
        raise HTTPException(status_code=400, detail="批量任务已完成")

    _request_batch_cancel(batch_id)
    return {"success": True, "message": "批量任务取消请求已提交，正在让它们有序收工"}


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
):
    """获取任务列表"""
    with get_db() as db:
        query = db.query(RegistrationTask)

        if status:
            query = query.filter(RegistrationTask.status == status)

        total = query.count()
        offset = (page - 1) * page_size
        tasks = query.order_by(RegistrationTask.created_at.desc()).offset(offset).limit(page_size).all()

        return TaskListResponse(
            total=total,
            tasks=[task_to_response(t, db=db) for t in tasks]
        )


@router.get("/tasks/{task_uuid}", response_model=RegistrationTaskResponse)
async def get_task(task_uuid: str):
    """获取任务详情"""
    with get_db() as db:
        task = crud.get_registration_task(db, task_uuid)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")
        return task_to_response(task, db=db)


@router.get("/tasks/{task_uuid}/logs")
async def get_task_logs(task_uuid: str):
    """获取任务日志"""
    with get_db() as db:
        task = crud.get_registration_task(db, task_uuid)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        logs = task.logs or ""
        result = task.result if isinstance(task.result, dict) else {}
        email = result.get("email")
        service_type = task.email_service.service_type if task.email_service else None
        email_service, email_service_name = _resolve_task_email_service(task, db=db)
        return {
            "task_uuid": task_uuid,
            "status": task.status,
            "email": email or _resolve_task_email(task),
            "email_service": service_type or email_service,
            "email_service_name": email_service_name,
            "logs": logs.split("\n") if logs else []
        }


@router.post("/tasks/{task_uuid}/cancel")
async def cancel_task(task_uuid: str):
    """取消任务"""
    with get_db() as db:
        task = crud.get_registration_task(db, task_uuid)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        if task.status not in ["pending", "running"]:
            raise HTTPException(status_code=400, detail="任务已完成或已取消")

        task_manager.cancel_task(task_uuid)
        task_manager.update_status(task_uuid, "cancelling", error="用户手动取消任务")
        task = crud.update_registration_task(
            db,
            task_uuid,
            status="cancelled",
            completed_at=datetime.utcnow(),
            error_message="用户手动取消任务",
        )

        return {"success": True, "message": "任务已取消"}


@router.delete("/tasks/{task_uuid}")
async def delete_task(task_uuid: str):
    """删除任务"""
    with get_db() as db:
        task = crud.get_registration_task(db, task_uuid)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        if task.status == "running":
            raise HTTPException(status_code=400, detail="无法删除运行中的任务")

        crud.delete_registration_task(db, task_uuid)

        return {"success": True, "message": "任务已删除"}


@router.get("/stats")
async def get_registration_stats():
    """获取注册统计信息"""
    with get_db() as db:
        from sqlalchemy import func

        # 按状态统计
        status_stats = db.query(
            RegistrationTask.status,
            func.count(RegistrationTask.id)
        ).group_by(RegistrationTask.status).all()

        # 今日统计
        today = datetime.utcnow().date()
        today_status_stats = db.query(
            RegistrationTask.status,
            func.count(RegistrationTask.id)
        ).filter(
            func.date(RegistrationTask.created_at) == today
        ).group_by(RegistrationTask.status).all()

        today_by_status = {status: count for status, count in today_status_stats}
        today_success = int(today_by_status.get("completed", 0))
        today_failed = int(today_by_status.get("failed", 0))
        # 今日“注册”口径：仅统计成功 + 失败（不包含 cancelled/running 等）
        today_total = today_success + today_failed
        today_success_rate = round((today_success / today_total) * 100, 1) if today_total > 0 else 0.0

        return {
            "by_status": {status: count for status, count in status_stats},
            "today_count": today_total,
            "today_total": today_total,
            "today_success": today_success,
            "today_failed": today_failed,
            "today_success_rate": today_success_rate,
            "today_by_status": today_by_status,
        }


@router.get("/available-services")
async def get_available_email_services():
    """
    获取可用于注册的邮箱服务列表

    返回所有已启用的邮箱服务，包括：
    - tempmail: 临时邮箱（无需配置）
    - outlook: 已导入的 Outlook 账户
    - moe_mail: 已配置的自定义域名服务
    """
    from ...database.models import EmailService as EmailServiceModel
    from ...config.settings import get_settings

    settings = get_settings()
    result = {
        "tempmail": {
            "available": True,
            "count": 1,
            "services": [{
                "id": None,
                "name": "Tempmail.lol",
                "type": "tempmail",
                "description": "临时邮箱，自动创建"
            }]
        },
        "outlook": {
            "available": False,
            "count": 0,
            "services": []
        },
        "moe_mail": {
            "available": False,
            "count": 0,
            "services": []
        },
        "temp_mail": {
            "available": False,
            "count": 0,
            "services": []
        },
        "duck_mail": {
            "available": False,
            "count": 0,
            "services": []
        },
        "freemail": {
            "available": False,
            "count": 0,
            "services": []
        },
        "imap_mail": {
            "available": False,
            "count": 0,
            "services": []
        },
        "yyds_mail": {
            "available": False,
            "count": 0,
            "services": []
        },
        "cloud_mail": {
            "available": False,
            "count": 0,
            "services": []
        }
    }

    with get_db() as db:
        # 获取 Outlook 账户
        outlook_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "outlook",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in outlook_services:
            config = service.config or {}
            result["outlook"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "outlook",
                "has_oauth": bool(config.get("client_id") and config.get("refresh_token")),
                "priority": service.priority
            })

        result["outlook"]["count"] = len(outlook_services)
        result["outlook"]["available"] = len(outlook_services) > 0

        # 获取自定义域名服务
        custom_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "moe_mail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in custom_services:
            config = service.config or {}
            result["moe_mail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "moe_mail",
                "default_domain": config.get("default_domain"),
                "priority": service.priority
            })

        result["moe_mail"]["count"] = len(custom_services)
        result["moe_mail"]["available"] = len(custom_services) > 0

        # 如果数据库中没有自定义域名服务，检查 settings
        if not result["moe_mail"]["available"]:
            if settings.custom_domain_base_url and settings.custom_domain_api_key:
                result["moe_mail"]["available"] = True
                result["moe_mail"]["count"] = 1
                result["moe_mail"]["services"].append({
                    "id": None,
                    "name": "默认自定义域名服务",
                    "type": "moe_mail",
                    "from_settings": True
                })

        # 获取 TempMail 服务（自部署 Cloudflare Worker 临时邮箱）
        temp_mail_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "temp_mail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in temp_mail_services:
            config = service.config or {}
            result["temp_mail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "temp_mail",
                "domain": config.get("domain"),
                "priority": service.priority
            })

        result["temp_mail"]["count"] = len(temp_mail_services)
        result["temp_mail"]["available"] = len(temp_mail_services) > 0

        duck_mail_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "duck_mail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in duck_mail_services:
            config = service.config or {}
            result["duck_mail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "duck_mail",
                "default_domain": config.get("default_domain"),
                "priority": service.priority
            })

        result["duck_mail"]["count"] = len(duck_mail_services)
        result["duck_mail"]["available"] = len(duck_mail_services) > 0

        freemail_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "freemail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in freemail_services:
            config = service.config or {}
            result["freemail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "freemail",
                "domain": config.get("domain"),
                "priority": service.priority
            })

        result["freemail"]["count"] = len(freemail_services)
        result["freemail"]["available"] = len(freemail_services) > 0

        imap_mail_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "imap_mail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in imap_mail_services:
            config = service.config or {}
            result["imap_mail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "imap_mail",
                "email": config.get("email"),
                "host": config.get("host"),
                "priority": service.priority
            })

        result["imap_mail"]["count"] = len(imap_mail_services)
        result["imap_mail"]["available"] = len(imap_mail_services) > 0

        yyds_mail_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "yyds_mail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in yyds_mail_services:
            config = service.config or {}
            result["yyds_mail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "yyds_mail",
                "default_domain": config.get("default_domain"),
                "priority": service.priority
            })

        result["yyds_mail"]["count"] = len(yyds_mail_services)
        result["yyds_mail"]["available"] = len(yyds_mail_services) > 0

        yyds_base_url = getattr(settings, "yyds_mail_base_url", None)
        yyds_api_key = getattr(settings, "yyds_mail_api_key", None)
        yyds_default_domain = getattr(settings, "yyds_mail_default_domain", None)
        if yyds_base_url and yyds_api_key:
            result["yyds_mail"]["services"].insert(0, {
                "id": None,
                "name": "YYDS Mail",
                "type": "yyds_mail",
                "default_domain": yyds_default_domain,
                "from_settings": True,
            })
            result["yyds_mail"]["count"] = len(result["yyds_mail"]["services"])
            result["yyds_mail"]["available"] = True

        # 获取 Cloud Mail 服务
        cloud_mail_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "cloud_mail",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        for service in cloud_mail_services:
            config = service.config or {}
            domain = config.get("domain")
            # 如果是列表，显示第一个域名
            if isinstance(domain, list) and domain:
                domain_display = domain[0]
            else:
                domain_display = domain
            
            result["cloud_mail"]["services"].append({
                "id": service.id,
                "name": service.name,
                "type": "cloud_mail",
                "domain": domain_display,
                "priority": service.priority
            })

        result["cloud_mail"]["count"] = len(cloud_mail_services)
        result["cloud_mail"]["available"] = len(cloud_mail_services) > 0

    return result


# ============== Outlook 批量注册 API ==============

@router.get("/outlook-accounts", response_model=OutlookAccountsListResponse)
async def get_outlook_accounts_for_registration():
    """
    获取可用于注册的 Outlook 账户列表

    返回所有已启用的 Outlook 服务，并检查每个邮箱是否已在 accounts 表中注册
    """
    from ...database.models import EmailService as EmailServiceModel
    from ...database.models import Account

    with get_db() as db:
        # 获取所有启用的 Outlook 服务
        outlook_services = db.query(EmailServiceModel).filter(
            EmailServiceModel.service_type == "outlook",
            EmailServiceModel.enabled == True
        ).order_by(EmailServiceModel.priority.asc()).all()

        accounts = []
        registered_count = 0
        unregistered_count = 0

        for service in outlook_services:
            config = service.config or {}
            email = config.get("email") or service.name
            normalized_email = str(email or "").strip().lower()

            # 检查是否已注册（查询 accounts 表）
            existing_account = db.query(Account).filter(
                func.lower(Account.email) == normalized_email
            ).first()

            is_registered = existing_account is not None
            if is_registered:
                registered_count += 1
            else:
                unregistered_count += 1

            accounts.append(OutlookAccountForRegistration(
                id=service.id,
                email=email,
                name=service.name,
                has_oauth=bool(config.get("client_id") and config.get("refresh_token")),
                is_registered=is_registered,
                registered_account_id=existing_account.id if existing_account else None
            ))

        return OutlookAccountsListResponse(
            total=len(accounts),
            registered_count=registered_count,
            unregistered_count=unregistered_count,
            accounts=accounts
        )


async def run_outlook_batch_registration(
    batch_id: str,
    service_ids: List[int],
    skip_registered: bool,
    proxy: Optional[str],
    interval_min: int,
    interval_max: int,
    concurrency: int = 1,
    mode: str = "pipeline",
    auto_upload_cpa: bool = False,
    cpa_service_ids: List[int] = None,
    auto_upload_sub2api: bool = False,
    sub2api_service_ids: List[int] = None,
    auto_upload_tm: bool = False,
    tm_service_ids: List[int] = None,
    registration_type: str = RoleTag.CHILD.value,
):
    """
    异步执行 Outlook 批量注册任务，复用通用并发逻辑

    将每个 service_id 映射为一个独立的 task_uuid，然后调用
    run_batch_registration 的并发逻辑
    """
    loop = task_manager.get_loop()
    if loop is None:
        loop = asyncio.get_event_loop()
        task_manager.set_loop(loop)

    # 预先为每个 service_id 创建注册任务记录
    task_uuids = []
    with get_db() as db:
        for service_id in service_ids:
            task_uuid = str(uuid.uuid4())
            crud.create_registration_task(
                db,
                task_uuid=task_uuid,
                proxy=proxy,
                email_service_id=service_id,
                batch_id=batch_id,
            )
            task_uuids.append(task_uuid)

    # 复用通用并发逻辑（outlook 服务类型，每个任务通过 email_service_id 定位账户）
    await run_batch_registration(
        batch_id=batch_id,
        task_uuids=task_uuids,
        email_service_type="outlook",
        proxy=proxy,
        email_service_config=None,
        email_service_id=None,   # 每个任务已绑定了独立的 email_service_id
        interval_min=interval_min,
        interval_max=interval_max,
        concurrency=concurrency,
        mode=mode,
        auto_upload_cpa=auto_upload_cpa,
        cpa_service_ids=cpa_service_ids,
        auto_upload_sub2api=auto_upload_sub2api,
        sub2api_service_ids=sub2api_service_ids,
        auto_upload_tm=auto_upload_tm,
        tm_service_ids=tm_service_ids,
        registration_type=registration_type,
    )


@router.post("/outlook-batch", response_model=OutlookBatchRegistrationResponse)
async def start_outlook_batch_registration(
    request: OutlookBatchRegistrationRequest,
    background_tasks: BackgroundTasks
):
    """
    启动 Outlook 批量注册任务

    - service_ids: 选中的 EmailService ID 列表
    - skip_registered: 是否自动跳过已注册邮箱（默认 True）
    - proxy: 代理地址
    - interval_min: 最小间隔秒数
    - interval_max: 最大间隔秒数
    """
    from ...database.models import EmailService as EmailServiceModel
    from ...database.models import Account

    # 验证参数
    if not request.service_ids:
        raise HTTPException(status_code=400, detail="请选择至少一个 Outlook 账户")

    if request.interval_min < 0 or request.interval_max < request.interval_min:
        raise HTTPException(status_code=400, detail="间隔时间参数无效")

    if not 1 <= request.concurrency <= 50:
        raise HTTPException(status_code=400, detail="并发数必须在 1-50 之间")

    if request.mode not in ("parallel", "pipeline"):
        raise HTTPException(status_code=400, detail="模式必须为 parallel 或 pipeline")

    # 过滤掉已注册的邮箱
    actual_service_ids = request.service_ids
    skipped_count = 0

    if request.skip_registered:
        actual_service_ids = []
        with get_db() as db:
            for service_id in request.service_ids:
                service = db.query(EmailServiceModel).filter(
                    EmailServiceModel.id == service_id
                ).first()

                if not service:
                    continue

                config = service.config or {}
                email = config.get("email") or service.name
                normalized_email = str(email or "").strip().lower()

                # 检查是否已注册
                existing_account = db.query(Account).filter(
                    func.lower(Account.email) == normalized_email
                ).first()

                if existing_account:
                    skipped_count += 1
                else:
                    actual_service_ids.append(service_id)

    if not actual_service_ids:
        return OutlookBatchRegistrationResponse(
            batch_id="",
            total=len(request.service_ids),
            skipped=skipped_count,
            to_register=0,
            service_ids=[]
        )

    # 创建批量任务
    batch_id = str(uuid.uuid4())

    with get_db() as db:
        crud.create_registration_batch(
            db,
            batch_id=batch_id,
            batch_type="outlook",
            mode=request.mode,
            total=len(actual_service_ids),
            email_service_type="outlook",
            proxy=request.proxy,
            interval_min=request.interval_min,
            interval_max=request.interval_max,
            concurrency=request.concurrency,
            request_payload=_model_dump(request),
        )
        crud.update_registration_batch(
            db,
            batch_id,
            skipped=skipped_count,
            updated_at=datetime.utcnow(),
        )

    # 初始化批量任务状态
    batch_tasks[batch_id] = {
        "batch_id": batch_id,
        "total": len(actual_service_ids),
        "completed": 0,
        "success": 0,
        "failed": 0,
        "skipped": skipped_count,
        "status": "pending",
        "error_message": None,
        "cancelled": False,
        "service_ids": actual_service_ids,
        "current_index": 0,
        "logs": [],
        "finished": False
    }

    # 在后台运行批量注册
    background_tasks.add_task(
        run_outlook_batch_registration,
        batch_id,
        actual_service_ids,
        request.skip_registered,
        request.proxy,
        request.interval_min,
        request.interval_max,
        request.concurrency,
        request.mode,
        request.auto_upload_cpa,
        request.cpa_service_ids,
        request.auto_upload_sub2api,
        request.sub2api_service_ids,
        request.auto_upload_tm,
        request.tm_service_ids,
        request.registration_type,
    )

    return OutlookBatchRegistrationResponse(
        batch_id=batch_id,
        total=len(request.service_ids),
        skipped=skipped_count,
        to_register=len(actual_service_ids),
        service_ids=actual_service_ids
    )


@router.get("/outlook-batch/{batch_id}")
async def get_outlook_batch_status(batch_id: str):
    """获取 Outlook 批量任务状态"""
    payload = _get_batch_status_payload(batch_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="批量任务不存在")
    return payload


@router.post("/outlook-batch/{batch_id}/cancel")
async def cancel_outlook_batch(batch_id: str):
    """取消 Outlook 批量任务"""
    payload = _get_batch_status_payload(batch_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="批量任务不存在")

    if payload.get("finished"):
        raise HTTPException(status_code=400, detail="批量任务已完成")

    _request_batch_cancel(batch_id)
    return {"success": True, "message": "批量任务取消请求已提交，正在让它们有序收工"}
