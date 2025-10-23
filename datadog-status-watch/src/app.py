import json
import os
import logging
import urllib.request
import urllib.error
import boto3
import time
import random
import socket
import ssl
from datetime import datetime, timezone

# ---- ログ設定（簡素）----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

STATUS_URL = os.getenv("STATUS_URL", "https://status.datadoghq.com/api/v2/status.json")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

# リトライ関連（必要に応じて環境変数で調整）
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "3"))   # 1回あたりのタイムアウト（短め）
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))            # 総リトライ回数
BACKOFF_BASE_SEC = float(os.getenv("BACKOFF_BASE_SEC", "0.5"))        # バックオフ基準
BACKOFF_CAP_SEC = float(os.getenv("BACKOFF_CAP_SEC", "3.0"))          # バックオフ上限

sns = boto3.client("sns")


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


def _fetch_status_with_retry(url: str) -> dict:
    last_err = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "dd-status-check/1.0",
                    "Accept": "application/json",
                },
            )
            # 1回あたり短めのタイムアウト
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                data = resp.read().decode("utf-8")
                return json.loads(data)

        except Exception as e:
            last_err = e
            # タイムアウト系はリトライ、それ以外は即時スロー
            if not _is_timeout_error(e):
                raise

            if attempt < HTTP_MAX_RETRIES:
                # 指数バックオフ＋ジッタ（上限あり）
                backoff = min(BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.2), BACKOFF_CAP_SEC)
                logger.warning(f"Timeout on attempt {attempt}/{HTTP_MAX_RETRIES}. Retrying in {backoff:.2f}s ...")
                time.sleep(backoff)
            else:
                # リトライ尽きた
                break

    # ここまで来たら失敗を投げる
    raise last_err


def _should_alert(status_payload: dict) -> tuple[bool, str, str]:
    """
    Returns (alert, indicator, description)
    indicator: "none" (OK), "minor", "major", "critical", "maintenance", etc.
    """
    status = status_payload.get("status", {})
    indicator = status.get("indicator", "unknown")
    description = status.get("description", "No description")
    alert = indicator != "none"
    return alert, indicator, description


def _publish_alert(indicator: str, description: str, payload: dict):
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set. Skipping SNS publish.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    subject = f"[Datadog Status] {indicator.upper()}: {description}"
    message = {
        "timestamp": now_iso,
        "indicator": indicator,
        "description": description,
        "source": STATUS_URL,
        "raw": payload,
    }
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=json.dumps(message, ensure_ascii=False, indent=2),
    )
    logger.info(f"SNS published: {subject}")


def lambda_handler(event, context):
    start = datetime.now(timezone.utc)
    logger.info("=== Datadog Status Check started ===")

    try:
        payload = _fetch_status_with_retry(STATUS_URL)
        alert, indicator, description = _should_alert(payload)
        logger.info(f"Datadog status: indicator={indicator}, description={description}")

        if alert:
            _publish_alert(indicator, description, payload)
        else:
            logger.info("No alert. (indicator == 'none')")

        elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        logger.info(f"=== Check completed successfully ({elapsed_ms} ms) ===")
        return {"ok": True, "indicator": indicator, "description": description}

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout, ssl.SSLError) as e:
        logger.error(f"HTTP/Timeout error while fetching status: {e}")
        # タイムアウトやHTTP系の失敗も通知したい場合のみ SNS（トピック未設定ならスキップ）
        if SNS_TOPIC_ARN:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="[Datadog Status] FETCH_ERROR",
                Message=json.dumps(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "error": str(e),
                        "source": STATUS_URL,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            logger.warning("SNS published: FETCH_ERROR")
        return {"ok": False, "error": str(e)}

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise
