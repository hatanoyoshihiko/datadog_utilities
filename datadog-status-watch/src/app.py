import json
import os
import logging
import urllib.request
import urllib.error
import time
import random
import socket
import ssl
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

# ---- ログ設定 ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ---- 設定 ----
# Datadog ステータス API（サマリー。未解決インシデント/コンポーネント情報を含む）
STATUS_URL = os.getenv("STATUS_URL", "https://status.datadoghq.com/api/v2/summary.json")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

# リトライ関連（必要に応じて環境変数で調整）
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "3"))   # 1回あたりのタイムアウト
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))            # リトライ回数
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "0.5"))        # バックオフ基準
BACKOFF_CAP_SEC = float(os.getenv("BACKOFF_CAP_SEC", "3.0"))          # バックオフ上限

import boto3
sns = boto3.client("sns")


# ---- ユーティリティ ----
def _is_timeout_error(err: Exception) -> bool:
    """タイムアウト系の例外かどうかを緩やかに判定"""
    if isinstance(err, TimeoutError):
        return True
    if isinstance(err, socket.timeout):
        return True
    if isinstance(err, urllib.error.URLError) and isinstance(getattr(err, "reason", None), socket.timeout):
        return True
    if isinstance(err, ssl.SSLError) and "timed out" in str(err).lower():
        return True
    return False


def _fetch_status_with_retry(url: str) -> Dict[str, Any]:
    """短いタイムアウト＋指数バックオフで summary.json を取得"""
    last_err: Exception | None = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "dd-status-check/1.0",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                data = resp.read().decode("utf-8")
                return json.loads(data)

        except Exception as e:
            last_err = e
            # タイムアウト系のみリトライ。それ以外は即時スロー
            if not _is_timeout_error(e):
                raise

            if attempt < HTTP_MAX_RETRIES:
                backoff = min(BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.2), BACKOFF_CAP_SEC)
                logger.warning(f"Timeout on attempt {attempt}/{HTTP_MAX_RETRIES}. Retrying in {backoff:.2f}s ...")
                time.sleep(backoff)
            else:
                break

    # ここまで来たら失敗を投げる
    assert last_err is not None
    raise last_err


# ---- アラート判定（summary.json 専用）----
def _should_alert_summary(payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    summary.json を前提。
    戻り値: (alert, summary_info)
      summary_info = {
        "indicator": str,               # ページの総合指標（none/ minor/ major/ critical）
        "description": str,             # ページの説明
        "page_updated_at": str | None,  # ページ更新時刻
        "incidents": [ { name, impact, status, shortlink, latest_update_body, latest_update_at } ],
        "degraded_components": [ { name, status } ],
      }
    """
    status = payload.get("status", {}) or {}
    indicator = status.get("indicator", "unknown")
    description = status.get("description", "No description")

    # 未解決インシデントを抽出
    incidents_out: List[Dict[str, Any]] = []
    for inc in (payload.get("incidents") or []):
        updates = inc.get("incident_updates") or []
        latest = None
        if updates:
            # created_at の最大を最新とみなす（文字列ISO想定）
            latest = max(updates, key=lambda u: u.get("created_at") or "")
        incidents_out.append({
            "name": inc.get("name"),
            "impact": inc.get("impact"),
            "status": inc.get("status"),
            "shortlink": inc.get("shortlink"),
            "latest_update_body": (latest or {}).get("body"),
            "latest_update_at": (latest or {}).get("created_at"),
        })

    # 劣化/障害中コンポーネントを抽出
    bad_statuses = {"degraded_performance", "partial_outage", "major_outage"}
    degraded_components = [
        {"name": c.get("name"), "status": c.get("status")}
        for c in (payload.get("components") or [])
        if c.get("status") in bad_statuses
    ]

    # 通知判定：indicator != none または 未解決インシデントが1件以上
    alert = (indicator != "none") or (len(incidents_out) > 0)

    summary_info = {
        "indicator": indicator,
        "description": description,
        "page_updated_at": (payload.get("page") or {}).get("updated_at"),
        "incidents": incidents_out,
        "degraded_components": degraded_components,
    }
    return alert, summary_info


# ---- 通知（障害のみ）----
def _publish_incident_alert(summary_info: Dict[str, Any], raw_payload: Dict[str, Any]) -> None:
    """障害情報のみを SNS に通知。FETCH_ERROR はここでは扱わない"""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set. Skipping SNS publish.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    subject = f"[Datadog Status] {summary_info['indicator'].upper()}: {summary_info['description']} (incidents: {len(summary_info['incidents'])})"

    # メール本文：運用しやすい最小構成（raw は必要なら残す）
    message = {
        "timestamp": now_iso,
        "page_updated_at": summary_info.get("page_updated_at"),
        "indicator": summary_info["indicator"],
        "description": summary_info["description"],
        "unresolved_incidents": summary_info["incidents"],
        "degraded_components": summary_info["degraded_components"],
        "source": STATUS_URL,
        # 解析で使う場合のみ raw を参照。不要ならコメントアウト可。
        "raw": raw_payload,
    }

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS Subject は最大 100 文字
        Message=json.dumps(message, ensure_ascii=False, indent=2),
    )
    logger.info(f"SNS published: {subject}")


# ---- Lambda ハンドラ ----
def lambda_handler(event, context):
    start = datetime.now(timezone.utc)
    logger.info("=== Datadog Status Check started ===")

    try:
        payload = _fetch_status_with_retry(STATUS_URL)
        alert, summary_info = _should_alert_summary(payload)

        logger.info(
            f"Datadog summary: indicator={summary_info['indicator']}, "
            f"desc={summary_info['description']}, incidents={len(summary_info['incidents'])}"
        )

        if alert:
            _publish_incident_alert(summary_info, payload)
        else:
            logger.info("No alert. (indicator == 'none' and no unresolved incidents)")

        elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        logger.info(f"=== Check completed successfully ({elapsed_ms} ms) ===")
        return {"ok": True, **summary_info, "elapsed_ms": elapsed_ms}

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout, ssl.SSLError) as e:
        # ★ 障害以外（ネットワーク/タイムアウト等）は通知しない：ログのみ
        logger.error(f"Fetch error ignored for alerting: {e}")
        return {"ok": False, "error": str(e)}

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise
