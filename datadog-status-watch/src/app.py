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
from typing import Any, Dict, Optional

# =========================
# 設定（環境変数）
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
STATUS_URL = os.getenv("STATUS_URL", "https://status.datadoghq.com/api/v2/status.json")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")  # 未設定なら通知しない（ログのみ）

# ネットワーク系
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "6"))   # 既定: 6秒
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))           # 既定: 3回
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "1.2"))       # バックオフ倍率

# 重複抑止（同一内容の連投防止）
DEDUP_TTL_MIN = int(os.getenv("DEDUP_TTL_MIN", "60"))  # 60分間は同一シグネチャなら抑止
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
            "User-Agent": "dd-status-check/status-json/1.0",
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

def _build_alert_subject(indicator: str, description: str) -> str:
    # 件名は「[Datadog Status] <description>」に統一
    desc = description or indicator or "Status changed"
    return f"[Datadog Status] {desc}"

def _build_alert_body(payload: Dict[str, Any], fetched_at: datetime) -> str:
    # /api/v2/status.json の内容をそのまま分かりやすく整形
    page = payload.get("page", {})
    status = payload.get("status", {})
    indicator = (status.get("indicator") or "-")
    description = (status.get("description") or "-")
    page_updated = _fmt_dt(page.get("updated_at"))

    lines = []
    lines.append(f"Fetched at (UTC): {_fmt_dt(fetched_at.isoformat())}")
    lines.append("")
    lines.append("=== Status ===")
    lines.append(f"indicator: {indicator}")         # none / minor / major / critical 等
    lines.append(f"description: {description}")     # All Systems Operational / Partial Outage 等
    lines.append("")
    lines.append("=== Page ===")
    lines.append(f"name: {page.get('name') or '-'}")
    lines.append(f"url:  {page.get('url') or '-'}")
    lines.append(f"updated_at: {page_updated}")
    lines.append("")
    lines.append("------------------------------------------------------------")
    lines.append("This email was sent by the Datadog status (status.json) watcher.")
    return "\n".join(lines)

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

def _publish_sns(subject: str, message: str) -> None:
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set. Skipping publish.")
        return
    import boto3
    sns = boto3.client("sns")
    resp = sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
    logger.info(f"SNS published: MessageId={resp.get('MessageId')}")

# =========================
# 判定ロジック（status.json 専用）
# =========================
def _is_alert_status(payload: Dict[str, Any]) -> bool:
    """
    /api/v2/status.json の 'status.indicator' を基準に判定する。
    - 'none' は「オールグリーン」 → 通知しない
    - それ以外（minor / major / critical 等） → 通知する
    """
    status = payload.get("status") or {}
    indicator = (status.get("indicator") or "").lower().strip()
    return indicator != "none" and indicator != ""

# =========================
# Lambda Handler
# =========================
def lambda_handler(event, context):
    start = _now_utc()
    logger.info("=== Datadog Status (status.json) check started ===")
    logger.info(f"URL={STATUS_URL}, retries={HTTP_MAX_RETRIES}, timeout={HTTP_TIMEOUT_SECONDS}s")

    try:
        payload = _fetch_status_with_retry(STATUS_URL)

        if _is_alert_status(payload):
            status = payload.get("status") or {}
            subject = _build_alert_subject(
                indicator=(status.get("indicator") or ""),
                description=(status.get("description") or ""),
            )
            body = _build_alert_body(payload, fetched_at=start)
            sig = _hash_signature(subject, body)

            if _should_suppress(sig, DEDUP_TTL_MIN):
                logger.info("Duplicate alert within TTL. Suppressing notify.")
            else:
                _publish_sns(subject, body)
                _save_last_sig(sig, _now_utc())
        else:
            logger.info("No alert. (indicator == 'none')")

        elapsed_ms = int((_now_utc() - start).total_seconds() * 1000)
        logger.info(f"=== Check completed successfully ({elapsed_ms} ms) ===")
        # 返却値は検証用（運用では CloudWatch Logs を参照）
        return {
            "ok": True,
            "alert": _is_alert_status(payload),
            "indicator": (payload.get("status") or {}).get("indicator"),
            "description": (payload.get("status") or {}).get("description"),
            "elapsed_ms": elapsed_ms,
        }

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout, ssl.SSLError) as e:
        # 取得失敗は通知しない（要件通り：タイムアウト等は“どうでもいい”）
        logger.error(f"Fetch error ignored for alerting: {e}")
        return {"ok": False, "error": str(e)}

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        # 予期せぬ例外は失敗として上位に伝播（アラーム等で検知）
        raise
