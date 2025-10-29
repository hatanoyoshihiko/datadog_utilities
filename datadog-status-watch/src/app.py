# src/app.py
import json
import os
import logging
import urllib.request
import urllib.error
import time
import random
import socket
import ssl
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

# =========================
# 設定（環境変数）
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
# summary.json を既定に
STATUS_URL = os.getenv("STATUS_URL", "https://status.datadoghq.com/api/v2/summary.json")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")  # 未設定なら通知しない（ログのみ）

# ネットワーク系
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "6"))   # 既定: 6秒
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))           # 既定: 3回
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "1.2"))       # バックオフ倍率

# 検知対象（true のとき components も見る）
ALERT_ON_COMPONENTS = os.getenv("ALERT_ON_COMPONENTS", "true").lower() in ("1", "true", "yes")
# under_maintenance は通知しない（必要なら true に）
ALERT_ON_UNDER_MAINT = os.getenv("ALERT_ON_UNDER_MAINT", "false").lower() in ("1", "true", "yes")

# 重複抑止（同一内容の連投防止）
DEDUP_TTL_MIN = int(os.getenv("DEDUP_TTL_MIN", "10"))  # 10分間は同一シグネチャなら抑止
DEDUP_PATH = "/tmp/dd_status_last_sig.json"

# ログ
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# =========================
# ユーティリティ
# =========================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _jitter(sec: float) -> float:
    # 0.8〜1.2倍程度のジッター
    return sec * random.uniform(0.8, 1.2)

def _read_text(url: str, timeout: int) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "dd-status-check/summary-json/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            raise urllib.error.HTTPError(url, resp.status, f"HTTP {resp.status}", resp.headers, None)
        return resp.read().decode("utf-8")

def _fetch_status_with_retry(url: str) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            body = _read_text(url, HTTP_TIMEOUT_SECONDS)
            return json.loads(body)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout, ssl.SSLError) as e:
            last_err = e
            if attempt < HTTP_MAX_RETRIES:
                sleep_sec = _jitter(BACKOFF_BASE_SEC ** attempt)
                logger.warning(f"[attempt {attempt}/{HTTP_MAX_RETRIES}] fetch failed: {e} -> sleep {sleep_sec:.2f}s")
                time.sleep(sleep_sec)
            else:
                logger.error(f"[attempt {attempt}/{HTTP_MAX_RETRIES}] fetch failed. giving up: {e}")
    # 到達したら最終エラー
    raise last_err or RuntimeError("unknown fetch error")

def _fmt_dt(s: Optional[str]) -> str:
    if not s:
        return "-"
    try:
        # Statuspage は ISO8601（Z）想定
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%SZ")  # UTC表記
    except Exception:
        return s

# =========================
# 解析ロジック（summary.json）
# =========================
def _extract_active_incidents(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    incidents:
      - status: investigating / identified / monitoring / resolved / postmortem
      - impact: critical / major / minor / none
    通知対象: status != resolved かつ impact != none
    """
    res: List[Dict[str, Any]] = []
    for inc in payload.get("incidents", []) or []:
        status = (inc.get("status") or "").lower()
        impact = (inc.get("impact") or "").lower()
        if status != "resolved" and impact != "none":
            res.append(inc)
    return res

def _extract_degraded_components(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    components:
      - status: operational / under_maintenance / degraded_performance / partial_outage / major_outage
    通知対象: operational 以外（under_maintenance は既定で除外）
    """
    res: List[Dict[str, Any]] = []
    for comp in payload.get("components", []) or []:
        status = (comp.get("status") or "").lower()
        if status == "operational":
            continue
        if status == "under_maintenance" and not ALERT_ON_UNDER_MAINT:
            continue
        res.append(comp)
    return res

# =========================
# 通知本文生成
# =========================
def _build_alert_subject(incidents: List[Dict[str, Any]], components: List[Dict[str, Any]]) -> str:
    if incidents:
        head = incidents[0].get("name") or "Datadog incidents"
        suffix = f" (+{len(incidents)-1})" if len(incidents) > 1 else ""
        return f"[Datadog Status] {head}{suffix}"
    if components:
        names = ", ".join([c.get("name") or "component" for c in components[:3]])
        more = f" (+{len(components)-3})" if len(components) > 3 else ""
        return f"[Datadog Status] Components degraded: {names}{more}"
    return "[Datadog Status] No issues"

def _build_alert_body(incidents: List[Dict[str, Any]], components: List[Dict[str, Any]], fetched_at: datetime, page: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"Fetched at (UTC): {_fmt_dt(fetched_at.isoformat())}")
    lines.append("")
    if incidents:
        lines.append("=== Incidents (status != resolved & impact != none) ===")
        for inc in incidents:
            name = inc.get("name") or "-"
            status = inc.get("status") or "-"
            impact = inc.get("impact") or "-"
            created = _fmt_dt(inc.get("created_at"))
            updated = _fmt_dt(inc.get("updated_at"))
            link = inc.get("shortlink") or inc.get("url") or "-"
            lines.append(f"- {name}")
            lines.append(f"  status: {status}, impact: {impact}")
            lines.append(f"  created_at: {created}, updated_at: {updated}")
            lines.append(f"  link: {link}")
            lines.append("")
    if components:
        lines.append("=== Components (operational を除く) ===" if not ALERT_ON_UNDER_MAINT else "=== Components (全状態) ===")
        for comp in components:
            name = comp.get("name") or "-"
            status = comp.get("status") or "-"
            updated = _fmt_dt(comp.get("updated_at") or comp.get("created_at"))
            lines.append(f"- {name}")
            lines.append(f"  status: {status}, updated_at: {updated}")
            lines.append("")
    if not incidents and not components:
        lines.append("No active incidents or degraded components detected.")
    # page 情報（任意）
    if page:
        lines.append("=== Page ===")
        lines.append(f"name: {page.get('name') or '-'}")
        lines.append(f"url:  {page.get('url') or '-'}")
        lines.append(f"updated_at: {_fmt_dt(page.get('updated_at'))}")
    lines.append("")
    lines.append("------------------------------------------------------------")
    lines.append("This email was sent by the Datadog status (summary.json) watcher.")
    return "\n".join(lines)

# =========================
# 重複抑止
# =========================
def _hash_signature(subject: str, body: str) -> str:
    h = hashlib.sha256()
    h.update(subject.encode("utf-8"))
    h.update(b"\n\n")
    h.update(body.encode("utf-8"))
    return h.hexdigest()

def _load_last_sig() -> Optional[Dict[str, Any]]:
    try:
        with open(DEDUP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _save_last_sig(sig: str, ts: datetime) -> None:
    payload = {"sig": sig, "ts": ts.isoformat()}
    try:
        with open(DEDUP_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception as e:
        logger.warning(f"failed to write dedup file: {e}")

def _should_suppress(sig: str, ttl_min: int) -> bool:
    data = _load_last_sig()
    if not data:
        return False
    if data.get("sig") != sig:
        return False
    try:
        ts = datetime.fromisoformat(data.get("ts"))
    except Exception:
        return False
    return (_now_utc() - ts) < timedelta(minutes=ttl_min)

# =========================
# SNS publish
# =========================
def _publish_sns(subject: str, message: str) -> None:
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set. Skipping publish.")
        return
    import boto3
    sns = boto3.client("sns")
    resp = sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
    logger.info(f"SNS published: MessageId={resp.get('MessageId')}")

# =========================
# Lambda Handler
# =========================
def lambda_handler(event, context):
    start = _now_utc()
    logger.info("=== Datadog Status (summary.json) check started ===")
    logger.info(f"URL={STATUS_URL}, retries={HTTP_MAX_RETRIES}, timeout={HTTP_TIMEOUT_SECONDS}s, components={ALERT_ON_COMPONENTS}")

    try:
        payload = _fetch_status_with_retry(STATUS_URL)

        incidents = _extract_active_incidents(payload)
        components = _extract_degraded_components(payload) if ALERT_ON_COMPONENTS else []
        page = payload.get("page") or {}

        if incidents or components:
            subject = _build_alert_subject(incidents, components)
            body = _build_alert_body(incidents, components, fetched_at=start, page=page)
            sig = _hash_signature(subject, body)

            if _should_suppress(sig, DEDUP_TTL_MIN):
                logger.info("Duplicate alert within TTL. Suppressing notify.")
            else:
                _publish_sns(subject, body)
                _save_last_sig(sig, _now_utc())
        else:
            logger.info("No alert. (no active incidents / degraded components)")

        elapsed_ms = int((_now_utc() - start).total_seconds() * 1000)
        logger.info(f"=== Check completed successfully ({elapsed_ms} ms) ===")
        return {"ok": True, "incidents": len(incidents), "components": len(components), "elapsed_ms": elapsed_ms}

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout, ssl.SSLError) as e:
        # 取得失敗は通知しない（要件通り：タイムアウト等は“どうでもいい”）
        logger.error(f"Fetch error ignored for alerting: {e}")
        return {"ok": False, "error": str(e)}

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        # 予期せぬ例外は失敗として上位に伝播（アラーム等で検知）
        raise
