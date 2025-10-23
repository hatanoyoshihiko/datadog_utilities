import json
import os
import logging
import urllib.request
import urllib.error
import boto3
from datetime import datetime, timezone

# ---- ログ設定（シンプル＋環境変数でレベル切替）----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

STATUS_URL = os.getenv("STATUS_URL", "https://status.datadoghq.com/api/v2/status.json")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

sns = boto3.client("sns")

def _fetch_status(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "dd-status-check/1.0"})
    # シンプル運用向けに少しだけ余裕を持たせる（必要ならtimeoutを5に戻す）
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        data = resp.read().decode("utf-8")
        return json.loads(data)

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
        payload = _fetch_status(STATUS_URL)
        alert, indicator, description = _should_alert(payload)
        logger.info(f"Datadog status: indicator={indicator}, description={description}")

        if alert:
            _publish_alert(indicator, description, payload)
        else:
            logger.info("No alert. (indicator == 'none')")

        elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        logger.info(f"=== Check completed successfully ({elapsed_ms} ms) ===")
        return {"ok": True, "indicator": indicator, "description": description}

    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        # API 通信エラーも検知したい場合は SNS 通知（Topic 未設定ならログのみ）
        logger.error(f"HTTP error while fetching status: {e}")
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
        # ここは敢えて raise（失敗を可視化したい時はそのまま）
        raise
