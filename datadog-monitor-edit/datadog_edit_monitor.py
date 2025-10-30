#!/usr/bin/env python3.13
# -*- coding: utf-8 -*-

"""
Datadog 既存モニターの一部設定を CSV 指定で更新するシンプル版
- 比較処理なし（既存値と一致していても PUT します）
- CSVで指定した項目のみ更新。空欄は触れません
- Datadog API v1 (PUT /api/v1/monitor/{monitor_id})
- 更新対象:
  - Evaluate Details: "Evaluate the" -> max|min|avg|sum（クエリ先頭関数）
  - Evaluate Details: "Of the query over the" -> last_1h 等（クエリ先頭の timeframe）
  - Evaluation options:
      - options.evaluation_delay（秒, int）
      - options.require_full_window（0/1 → bool）
- 実行前に「指定された項目」を表示して yes で実行
- 実行結果は datadog_monitor.log に成功/失敗を記録
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import urllib.request
import urllib.error

DATADOG_API_BASE = "https://api.datadoghq.com/api/v1"
LOG_FILE = "datadog_monitor.log"

# ---- ログ設定 ----
logger = logging.getLogger("dd_monitor_updater")
logger.setLevel(logging.INFO)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_fh)

# ---- CSV 行 ----
@dataclass
class CsvRow:
    monitor_id: int
    evaluate_the: Optional[str] = None   # max|min|avg|sum
    timeframe: Optional[str] = None      # e.g. last_1h
    evaluation_delay: Optional[int] = None
    require_full_window: Optional[int] = None     # 0 or 1

# ---- HTTP ----
def http_request_json(
    method: str,
    url: str,
    headers: Dict[str, str],
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 60
) -> Tuple[int, Dict[str, Any] | str]:
    data = None
    h = dict(headers)
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        h["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, method=method, headers=h, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8")
            try:
                return status, json.loads(body) if body else {}
            except json.JSONDecodeError:
                return status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except urllib.error.URLError as e:
        return 0, f"URLError: {e.reason}"

def get_monitor(monitor_id: int, headers: Dict[str, str]) -> Dict[str, Any]:
    url = f"{DATADOG_API_BASE}/monitor/{monitor_id}"
    status, resp = http_request_json("GET", url, headers)
    if status != 200:
        raise RuntimeError(f"GET monitor {monitor_id} failed: HTTP {status} {resp}")
    return resp

def put_monitor(monitor_id: int, headers: Dict[str, str], payload: Dict[str, Any]) -> Tuple[int, Any]:
    url = f"{DATADOG_API_BASE}/monitor/{monitor_id}"
    return http_request_json("PUT", url, headers, payload)

# ---- クエリ先頭置換 ----
# 例: avg(last_1h):sum:aws.elb.httpcode_elb_5xx{*} > 1
QUERY_HEAD_PATTERN = re.compile(r"""
    ^\s*
    (?P<func>avg|sum|max|min)
    \(
        (?P<time>last_[0-9]+[mhdw])
    \)
    \s*:\s*
    (?P<rest>.+)
    $
""", re.IGNORECASE | re.VERBOSE)
VALID_FUNC = {"max", "min", "avg", "sum"}

def rewrite_query_head(query: str, func_opt: Optional[str], timeframe_opt: Optional[str]) -> str:
    """
    先頭の関数/timeframeのみCSV指定に従って置換。指定なしは現状維持。
    クエリが想定形でない場合はそのまま返す（比較は行わない）。
    """
    m = QUERY_HEAD_PATTERN.match(query)
    if not m:
        return query
    out_func = m.group("func")
    out_time = m.group("time")

    if func_opt:
        f = func_opt.lower()
        if f not in VALID_FUNC:
            raise ValueError(f"evaluate_the は {sorted(VALID_FUNC)} のいずれか: got '{func_opt}'")
        out_func = f
    if timeframe_opt:
        out_time = timeframe_opt
    return f"{out_func}({out_time}):{m.group('rest')}"

# ---- CSV ----
def load_csv(path: str) -> list[CsvRow]:
    rows: list[CsvRow] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, r in enumerate(reader, start=2):
            if not r.get("monitor_id"):
                continue
            try:
                monitor_id = int(str(r["monitor_id"]).strip())
            except ValueError:
                raise ValueError(f"{path}:{i} monitor_id は整数で指定してください: {r.get('monitor_id')}")
            evaluate_the = (r.get("evaluate_the") or "").strip() or None
            timeframe = (r.get("timeframe") or "").strip() or None

            ed_raw = (r.get("evaluation_delay") or "").strip()
            evaluation_delay = int(ed_raw) if ed_raw else None

            rfw_raw = (r.get("require_full_window") or "").strip()
            if rfw_raw == "":
                require_full_window = None
            else:
                if rfw_raw not in {"0", "1"}:
                    raise ValueError(f"{path}:{i} require_full_window は 0 or 1 で指定してください: {rfw_raw}")
                require_full_window = int(rfw_raw)

            rows.append(CsvRow(
                monitor_id=monitor_id,
                evaluate_the=evaluate_the,
                timeframe=timeframe,
                evaluation_delay=evaluation_delay,
                require_full_window=require_full_window,
            ))
    return rows

# ---- メイン ----
def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Datadog Monitor 設定更新（比較なしシンプル版）")
    parser.add_argument("--csv", default="monitors.csv",
                        help="更新対象CSV（省略時: monitors.csv）")
    parser.add_argument("--site", default="datadoghq.com",
                        help="Datadog サイト（例: datadoghq.com / datadoghq.eu / us3.datadoghq.com 等）")
    args = parser.parse_args(argv)

    if not os.path.exists(args.csv):
        print(f"CSVファイルが見つかりません: {args.csv}", file=sys.stderr)
        print("実行ディレクトリに monitors.csv を置くか、--csv で指定してください。", file=sys.stderr)
        return 1

    # キー入力（表示）
    api_key = input("Datadog API Key: ").strip()
    app_key = input("Datadog Application Key: ").strip()
    if not api_key or not app_key:
        print("API Key と Application Key は必須です。", file=sys.stderr)
        return 2

    global DATADOG_API_BASE
    DATADOG_API_BASE = f"https://api.{args.site}/api/v1"

    headers = {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
        "Accept": "application/json",
    }

    # CSV読込
    try:
        rows = load_csv(args.csv)
    except Exception as e:
        print(f"CSV 読み込みエラー: {e}", file=sys.stderr)
        return 3

    if not rows:
        print("CSV に有効な行がありません。", file=sys.stderr)
        return 4

    # 実行前の“指定内容”プレビュー（比較なし）
    print("\n=== 実行前プレビュー（CSV指定内容） ===")
    for r in rows:
        print(f"- Monitor ID: {r.monitor_id}")
        if r.evaluate_the or r.timeframe:
            print(f"  query.head: func={r.evaluate_the or '(既存維持)'} timeframe={r.timeframe or '(既存維持)'}")
        else:
            print("  query.head: （指定なし=既存維持）")
        if any(v is not None for v in (r.evaluation_delay, r.require_full_window)):
            if r.evaluation_delay is not None:
                print(f"  options.evaluation_delay: -> {r.evaluation_delay}")
            if r.require_full_window is not None:
                print(f"  options.require_full_window: -> {bool(r.require_full_window)}")
        else:
            print("  options: （指定なし=既存維持）")

    ans = input("\n上記の更新を一括で実行しますか？（yes/no）: ").strip().lower()
    if ans != "yes":
        print("キャンセルしました。変更は行いません。")
        return 0

    # 実行
    print("\n--- 更新実行 ---")
    exit_code = 0
    for r in rows:
        try:
            # 現在のモニターを取得（比較はしないが、クエリと options 基準のために取得）
            mon = get_monitor(r.monitor_id, headers)

            payload: Dict[str, Any] = {}

            # query 先頭の関数/timeframe だけ置換（CSVに指定がある場合のみ）
            if r.evaluate_the or r.timeframe:
                cur_q = mon.get("query", "")
                new_q = rewrite_query_head(cur_q, r.evaluate_the, r.timeframe)
                payload["query"] = new_q  # 比較せず常に送る（先頭形状でない場合はそのまま）

            # options は現在値をベースに指定キーのみ上書き
            if (r.evaluation_delay is not None) or (r.require_full_window is not None):
                opts = dict(mon.get("options", {}) or {})
                if r.evaluation_delay is not None:
                    opts["evaluation_delay"] = r.evaluation_delay
                if r.require_full_window is not None:
                    opts["require_full_window"] = bool(r.require_full_window)
                payload["options"] = opts

            if not payload:
                # 本当に何も指定がない行はスキップ（PUTしない）
                logger.info(f"[SKIP] monitor_id={r.monitor_id} CSVに更新指定なしのためスキップ")
                print(f"SKIP {r.monitor_id}: CSVに更新指定がありません")
                continue

            status, resp = put_monitor(r.monitor_id, headers, payload)
            if 200 <= status < 300:
                logger.info(f"[SUCCESS] monitor_id={r.monitor_id} 更新成功")
                print(f"OK {r.monitor_id}: 更新成功")
            else:
                logger.error(f"[FAIL] monitor_id={r.monitor_id} HTTP {status} {resp}")
                print(f"FAIL {r.monitor_id}: HTTP {status} {resp}")
                exit_code = 1

        except Exception as e:
            logger.error(f"[ERROR] monitor_id={r.monitor_id} {e}")
            print(f"ERROR {r.monitor_id}: {e}")
            exit_code = 1

    print(f"\n結果は {LOG_FILE} を参照してください。")
    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
