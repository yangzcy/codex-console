"""
动态代理获取模块
支持通过外部 API 获取代理 URL，并上报任务结果。
"""

import logging
import re
import json
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


_PROXY_HEALTH_FILE = "dynamic_proxy_health.json"


def _health_store_path() -> Path:
    app_root = Path(__file__).resolve().parents[2]
    data_dir = app_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / _PROXY_HEALTH_FILE


def _load_proxy_health() -> Dict[str, Any]:
    path = _health_store_path()
    if not path.exists():
        return {"proxies": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("proxies"), dict):
            return data
    except Exception as exc:
        logger.warning("读取动态代理健康池失败: %s", exc)
    return {"proxies": {}}


def _save_proxy_health(payload: Dict[str, Any]) -> None:
    path = _health_store_path()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def _proxy_cooldown_seconds(reason: str, consecutive_failures: int) -> int:
    reason_key = str(reason or "").strip().lower()
    failures = max(1, int(consecutive_failures or 1))
    if reason_key == "register_create_account_retryable":
        if failures >= 3:
            return 2 * 3600
        if failures >= 2:
            return 45 * 60
        return 15 * 60
    if reason_key in {"auth_openai_unreachable", "token_exchange_fail"}:
        return 15 * 60
    if reason_key in {"connect_timeout", "http_429"}:
        return min(30 * 60, 120 * failures)
    if reason_key == "unknown_error":
        return min(15 * 60, 60 * failures)
    return 0


def _record_local_proxy_result(proxy_url: str, *, success: bool, reason: str, detail: str = "") -> None:
    runtime_proxy = str(proxy_url or "").strip()
    if not runtime_proxy:
        return

    now_ts = time.time()
    now_iso = datetime.now().isoformat()
    payload = _load_proxy_health()
    proxies = payload.setdefault("proxies", {})
    state = proxies.setdefault(runtime_proxy, {})
    state["updated_at"] = now_iso
    state["last_reason"] = str(reason or "").strip()
    state["last_detail"] = str(detail or "").strip()[:500]

    if success:
        state["success_count"] = int(state.get("success_count") or 0) + 1
        state["consecutive_failures"] = 0
        state["cooldown_until"] = 0
        _save_proxy_health(payload)
        return

    state["fail_count"] = int(state.get("fail_count") or 0) + 1
    state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1
    cooldown_seconds = _proxy_cooldown_seconds(reason, state["consecutive_failures"])
    if cooldown_seconds > 0:
        state["cooldown_until"] = max(float(state.get("cooldown_until") or 0.0), now_ts + cooldown_seconds)
        logger.warning(
            "动态代理进入冷却: %s, reason=%s, cooldown=%ss",
            runtime_proxy,
            reason,
            cooldown_seconds,
        )
    _save_proxy_health(payload)


def _is_proxy_cooling(proxy_url: str) -> bool:
    runtime_proxy = str(proxy_url or "").strip()
    if not runtime_proxy:
        return False
    payload = _load_proxy_health()
    state = payload.get("proxies", {}).get(runtime_proxy, {})
    return float(state.get("cooldown_until") or 0.0) > time.time()


def is_proxy_cooling(proxy_url: str) -> bool:
    return _is_proxy_cooling(proxy_url)


def _rewrite_loopback_proxy_host(proxy_url: str, api_url: str) -> str:
    """When the scheduler is reached via a host gateway IP, rewrite loopback proxy hosts to that same host."""
    try:
        parsed_proxy = urlparse(proxy_url)
        parsed_api = urlparse(api_url)
        proxy_host = str(parsed_proxy.hostname or "").strip().lower()
        api_host = str(parsed_api.hostname or "").strip()
        if proxy_host not in {"127.0.0.1", "localhost"} or not api_host:
            return proxy_url

        netloc = parsed_proxy.netloc
        if "@" in netloc:
            auth, hostport = netloc.rsplit("@", 1)
            _, _, port = hostport.partition(":")
            new_hostport = f"{api_host}:{port}" if port else api_host
            new_netloc = f"{auth}@{new_hostport}"
        else:
            _, _, port = netloc.partition(":")
            new_netloc = f"{api_host}:{port}" if port else api_host

        rewritten = urlunparse(parsed_proxy._replace(netloc=new_netloc))
        logger.info("动态代理地址改写: %s -> %s", proxy_url, rewritten)
        return rewritten
    except Exception as exc:
        logger.warning("动态代理地址改写失败，继续使用原地址: %s", exc)
        return proxy_url


def fetch_dynamic_proxy_with_meta(
    api_url: str,
    api_key: str = "",
    api_key_header: str = "X-API-Key",
    result_field: str = "",
) -> Tuple[Optional[str], str]:
    """
    从代理 API 获取代理 URL

    Args:
        api_url: 代理 API 地址，响应应为代理 URL 字符串或含代理 URL 的 JSON
        api_key: API 密钥（可选）
        api_key_header: API 密钥请求头名称
        result_field: 从 JSON 响应中提取代理 URL 的字段路径，支持点号分隔（如 "data.proxy"），留空则使用响应原文

    Returns:
        Tuple[proxy_url, proxy_id]
    """
    try:
        from curl_cffi import requests as cffi_requests
        import json

        headers = {}
        if api_key:
            headers[api_key_header] = api_key

        response = cffi_requests.get(
            api_url,
            headers=headers,
            timeout=10,
            impersonate="chrome110"
        )

        if response.status_code != 200:
            logger.warning(f"动态代理 API 返回错误状态码: {response.status_code}")
            return None, ""

        text = response.text.strip()
        proxy_id = ""

        # 尝试解析 JSON
        if result_field or text.startswith("{") or text.startswith("["):
            try:
                data = json.loads(text)
                if isinstance(data, dict):
                    proxy_id = str(data.get("proxy_id") or "").strip()
                if result_field:
                    # 按点号路径逐层提取
                    for key in result_field.split("."):
                        if isinstance(data, dict):
                            data = data.get(key)
                        elif isinstance(data, list) and key.isdigit():
                            data = data[int(key)]
                        else:
                            data = None
                        if data is None:
                            break
                    proxy_url = str(data).strip() if data is not None else None
                else:
                    # 无指定字段，尝试常见键名
                    for key in ("proxy", "url", "proxy_url", "data", "ip"):
                        val = data.get(key) if isinstance(data, dict) else None
                        if val:
                            proxy_url = str(val).strip()
                            break
                    else:
                        proxy_url = text
            except (ValueError, AttributeError):
                proxy_url = text
        else:
            proxy_url = text

        if not proxy_url:
            logger.warning("动态代理 API 返回空代理 URL")
            return None, proxy_id

        # 若未包含协议头，默认加 http://
        if not re.match(r'^(http|socks5)://', proxy_url):
            proxy_url = "http://" + proxy_url

        proxy_url = _rewrite_loopback_proxy_host(proxy_url, api_url)

        logger.info(f"动态代理获取成功: {proxy_url[:40]}..." if len(proxy_url) > 40 else f"动态代理获取成功: {proxy_url}")
        return proxy_url, proxy_id

    except Exception as e:
        logger.error(f"获取动态代理失败: {e}")
        return None, ""


def fetch_dynamic_proxy(api_url: str, api_key: str = "", api_key_header: str = "X-API-Key", result_field: str = "") -> Optional[str]:
    proxy_url, _ = fetch_dynamic_proxy_with_meta(
        api_url=api_url,
        api_key=api_key,
        api_key_header=api_key_header,
        result_field=result_field,
    )
    return proxy_url


def get_dynamic_proxy_for_task() -> Tuple[Optional[str], str]:
    """为任务获取动态代理及其代理池标识。"""
    from ..config.settings import get_settings

    settings = get_settings(force_reload=True)
    if not settings.proxy_dynamic_enabled or not settings.proxy_dynamic_api_url:
        return None, ""

    logger.info(
        "注册代理决策: 动态代理已启用，准备请求代理 API: %s",
        settings.proxy_dynamic_api_url,
    )
    api_key = settings.proxy_dynamic_api_key.get_secret_value() if settings.proxy_dynamic_api_key else ""
    cooled_candidate: Tuple[Optional[str], str] = (None, "")
    for attempt in range(1, 5):
        proxy_url, proxy_id = fetch_dynamic_proxy_with_meta(
            api_url=settings.proxy_dynamic_api_url,
            api_key=api_key,
            api_key_header=settings.proxy_dynamic_api_key_header,
            result_field=settings.proxy_dynamic_result_field,
        )
        if not proxy_url:
            continue
        if _is_proxy_cooling(proxy_url):
            if not cooled_candidate[0]:
                cooled_candidate = (proxy_url, proxy_id)
            logger.warning(
                "注册代理决策: 动态代理命中本地冷却，跳过第 %s 次返回的代理: %s",
                attempt,
                proxy_url[:80] + "..." if len(proxy_url) > 80 else proxy_url,
            )
            continue
        logger.info(
            "注册代理决策: 动态代理获取成功，最终使用代理: %s",
            proxy_url[:80] + "..." if len(proxy_url) > 80 else proxy_url,
        )
        return proxy_url, proxy_id

    if cooled_candidate[0]:
        logger.warning(
            "注册代理决策: 动态代理连续返回冷却中的代理，兜底放行最后候选: %s",
            cooled_candidate[0][:80] + "..." if len(cooled_candidate[0]) > 80 else cooled_candidate[0],
        )
        return cooled_candidate

    logger.warning("动态代理获取失败，回退到静态代理")
    return None, ""


def get_proxy_url_for_task() -> Optional[str]:
    """
    为注册任务获取代理 URL。
    优先使用动态代理（若启用），否则使用静态代理配置。

    Returns:
        代理 URL 或 None
    """
    from ..config.settings import get_settings
    settings = get_settings(force_reload=True)

    # 优先使用动态代理
    if settings.proxy_dynamic_enabled and settings.proxy_dynamic_api_url:
        proxy_url, _ = get_dynamic_proxy_for_task()
        if proxy_url:
            return proxy_url
    else:
        logger.info(
            "注册代理决策: 动态代理未启用或未配置 API，enabled=%s api_url=%s",
            bool(settings.proxy_dynamic_enabled),
            str(settings.proxy_dynamic_api_url or "").strip() or "-",
        )

    # 使用静态代理
    static_proxy = settings.proxy_url
    if static_proxy:
        logger.info(
            "注册代理决策: 使用静态代理: %s",
            static_proxy[:80] + "..." if len(static_proxy) > 80 else static_proxy,
        )
    else:
        logger.warning("注册代理决策: 未获取到任何代理，将直接使用当前服务器出口")
    return static_proxy


def report_dynamic_proxy_result(
    *,
    report_url: str,
    proxy_url: str,
    task_id: str = "",
    success: bool,
    reason: str = "",
    detail: str = "",
    purpose: str = "openai_register",
    proxy_id: str = "",
    api_key: str = "",
    api_key_header: str = "X-API-Key",
) -> bool:
    """向动态代理调度层上报任务结果。"""
    runtime_proxy = str(proxy_url or "").strip()
    _record_local_proxy_result(
        runtime_proxy,
        success=bool(success),
        reason=str(reason or "").strip(),
        detail=str(detail or "").strip(),
    )

    try:
        from curl_cffi import requests as cffi_requests

        target_url = str(report_url or "").strip()
        if not target_url or not runtime_proxy:
            return False

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers[api_key_header] = api_key

        payload = {
            "proxy_id": str(proxy_id or "").strip(),
            "proxy": runtime_proxy,
            "task_id": str(task_id or "").strip(),
            "purpose": str(purpose or "openai_register").strip(),
            "success": bool(success),
            "reason": str(reason or "").strip(),
            "detail": str(detail or "").strip(),
        }

        response = cffi_requests.post(
            target_url,
            headers=headers,
            json=payload,
            timeout=10,
            impersonate="chrome110",
        )
        if response.status_code != 200:
            logger.warning("动态代理结果上报失败: status=%s body=%s", response.status_code, response.text[:200])
            return False

        logger.info("动态代理结果上报成功: task=%s success=%s reason=%s", payload["task_id"], payload["success"], payload["reason"])
        return True
    except Exception as e:
        logger.warning("动态代理结果上报异常: %s", e)
        return False
