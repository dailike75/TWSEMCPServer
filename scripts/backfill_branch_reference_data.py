#!/usr/bin/env python3
"""回填分點資料庫的參考表：券商總公司（brokers、broker_code）與股票主檔（stocks）。

資料來源為證交所公開資料：證券商總公司基本資料（brokerService/brokerList）、證券商分公司
基本資料（OpenData_BRK02），以及上市／上櫃股票清單。分點與券商的歸屬無法由代號前綴推得
（1020 合庫與 1030 土銀同前綴卻是不同券商），因此改以分公司名稱「總公司-分公司」的前綴
比對總公司名稱；無法確定者保留空值並列出，不猜測。詳見 utils/broker_mapping.py。

只會寫入 brokers、broker_branches.broker_code、stocks 三處，不會動到 branch_daily_trades、
branch_stock_daily、import_log 等交易資料，而且三個語句在同一個 transaction 裡。

這是腳本而不是 MCP 工具：本 server 的每一支工具都是唯讀的（tools/__init__.py 一律標注
READ_ONLY，tests/unit/test_tool_registration_unit.py 強制），會寫入資料庫的動作不該放進
工具目錄裡讓模型隨手呼叫。

用法（預設只試算，不寫入）：

    BRANCH_DB_URL=... python scripts/backfill_branch_reference_data.py
    BRANCH_DB_URL=... python scripts/backfill_branch_reference_data.py --target brokers
    BRANCH_DB_WRITE_URL=... python scripts/backfill_branch_reference_data.py --apply

查詢用的 BRANCH_DB_URL 建議指向唯讀 role；實際寫入請另外設 BRANCH_DB_WRITE_URL
（未設定時退回 BRANCH_DB_URL）。
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.branch_reference import ReferenceDataUnavailable, apply, plan  # noqa: E402
from utils.api_client import TWSEAPIClient  # noqa: E402
from utils.neon_db import BranchDBUnavailable, get_branch_db  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="回填分點資料庫的參考表（預設只試算，不寫入）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--target", choices=("all", "brokers", "stocks"), default="all",
        help='要回填哪些表："all"（預設）、"brokers"（券商總公司與 broker_code）、"stocks"（股票主檔）',
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="實際寫入。未指定時只印出將變更的內容，完全不寫入",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="印出 HTTP 與 SQL 的 log")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    db = get_branch_db()
    if not db.configured:
        print(
            "尚未設定分點資料庫連線。請在環境變數 BRANCH_DB_URL 填入 PostgreSQL 連線字串"
            "（例如 Neon 的 connection string）後再執行。",
            file=sys.stderr,
        )
        return 2
    if args.apply and not db.write_configured:
        print(
            "未設定可寫入的連線字串。請設定 BRANCH_DB_WRITE_URL（或讓 BRANCH_DB_URL 指向"
            "具寫入權限的角色）後再加 --apply。",
            file=sys.stderr,
        )
        return 2

    try:
        result = plan(TWSEAPIClient(), db, args.target)
    except (ReferenceDataUnavailable, BranchDBUnavailable) as exc:
        print(f"查詢失敗：{exc}", file=sys.stderr)
        return 1

    print("【實際寫入】" if args.apply else "【試算】尚未寫入任何資料")
    print(result.describe())

    if not result.statements:
        print("\n沒有需要變更的資料。")
        return 0

    if not args.apply:
        print("\n確認無誤後請加上 --apply 實際寫入：")
        print(f"    python scripts/backfill_branch_reference_data.py --target {args.target} --apply")
        return 0

    try:
        applied = apply(db, result.statements)
    except BranchDBUnavailable as exc:
        print(f"寫入失敗，已全部回滾：{exc}", file=sys.stderr)
        return 1
    print("\n寫入完成（單一 transaction）：")
    for line in applied:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
