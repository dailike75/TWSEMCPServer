"""Plan the branch database's reference tables from TWSE's own published data.

The 分點進出 database (``branch_trades_schema.sql``) keeps three reference tables that the
import pipeline does not fill: ``brokers`` (券商總公司), ``broker_branches.broker_code`` (which
firm each 分點 belongs to) and ``stocks`` (code → name → market). Until they are filled, the
branch tools can only show codes and cannot aggregate by firm.

This module decides *what would change*; it writes nothing. ``scripts/backfill_branch_reference_data.py``
is the only caller that applies the plan, and it does so in one transaction. Keeping the planning
here rather than in the script means it is importable and unit-testable like any other service.

Why a script rather than an MCP tool: every tool in this server is read-only — ``tools/__init__.py``
annotates them all ``READ_ONLY`` and ``tests/unit/test_tool_registration_unit.py`` enforces it — so a
tool that writes to a database does not belong in the catalog.

Sources: ``brokerService/brokerList`` (總公司), ``opendata/OpenData_BRK02`` (分公司) and the
listed+OTC snapshot (``services.market_snapshot``). The branch → firm resolution rules, and why
the code prefix cannot be used, are in ``utils/broker_mapping.py``.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple

from utils.api_client import TWSEAPIClient
from utils.broker_mapping import resolve_broker_mapping
from utils.neon_db import BranchDB, Statement

from .market_snapshot import LISTED, OTC, market_snapshot

logger = logging.getLogger(__name__)

HQ_ENDPOINT = "/brokerService/brokerList"
BRANCH_ENDPOINT = "/opendata/OpenData_BRK02"

HQ_CODE_FIELD, HQ_NAME_FIELD = "Code", "Name"
BRANCH_CODE_FIELD, BRANCH_NAME_FIELD = "證券商代號", "證券商名稱"

MARKET_BY_LABEL = {LISTED: "TWSE", OTC: "TPEx"}

UPSERT_BROKER_SQL = """
INSERT INTO brokers (broker_code, broker_name)
VALUES (%s, %s)
ON CONFLICT (broker_code) DO UPDATE SET broker_name = EXCLUDED.broker_name
"""

UPDATE_BRANCH_SQL = """
UPDATE broker_branches
SET broker_code = %s, updated_at = now()
WHERE branch_code = %s AND broker_code IS DISTINCT FROM %s
"""

UPSERT_STOCK_SQL = """
INSERT INTO stocks (stock_id, stock_name, market)
VALUES (%s, %s, %s)
ON CONFLICT (stock_id) DO UPDATE
SET stock_name = EXCLUDED.stock_name, market = EXCLUDED.market
"""

# brokers.broker_name / stocks.stock_name are VARCHAR(32). A longer upstream name would abort the
# whole transaction, so it is truncated rather than allowed to fail the batch.
NAME_MAX = 32

METHOD_NAMES = {
    "exact": "總公司名稱精確比對",
    "hq_name_longer": "總公司名稱前綴比對",
    "name_prefix": "全名前綴比對",
    "headquarters": "本身即總公司",
    "db_name_prefix": "既有分點名稱比對",
    "sibling_prefix": "兄弟分點代號一致",
}


class ReferenceDataUnavailable(RuntimeError):
    """TWSE did not serve the reference data this plan needs."""


@dataclass
class Plan:
    """What a backfill would change. ``statements`` is empty when nothing would."""

    statements: List[Statement] = field(default_factory=list)
    report: List[str] = field(default_factory=list)

    def extend(self, other: "Plan") -> None:
        self.statements.extend(other.statements)
        self.report.extend(other.report)

    def describe(self) -> str:
        return "\n".join(self.report)


def describe(statement: Statement) -> str:
    """A human label for one planned statement, for the applied-rows report."""
    sql = statement.sql.strip()
    if sql.startswith("INSERT INTO brokers"):
        return "brokers 券商總公司"
    if sql.startswith("UPDATE broker_branches"):
        return "broker_branches.broker_code 分點對應"
    if sql.startswith("INSERT INTO stocks"):
        return "stocks 股票主檔"
    return sql.splitlines()[0]


def plan_brokers(client: TWSEAPIClient, db: BranchDB) -> Plan:
    """Resolve 分點 → 券商總公司 and plan the ``brokers`` / ``broker_code`` writes."""
    hq_raw = client.fetch_data(HQ_ENDPOINT) or []
    branch_raw = client.fetch_data(BRANCH_ENDPOINT) or []
    if not hq_raw or not branch_raw:
        raise ReferenceDataUnavailable(
            "無法取得證交所券商參考資料（brokerService/brokerList 或 OpenData_BRK02 回傳空值）"
        )

    hq_rows: Sequence[Tuple[str, str]] = [
        (str(r.get(HQ_CODE_FIELD, "")).strip(), str(r.get(HQ_NAME_FIELD, "")).strip())
        for r in hq_raw
    ]
    branch_rows: Sequence[Tuple[str, str]] = [
        (str(r.get(BRANCH_CODE_FIELD, "")).strip(), str(r.get(BRANCH_NAME_FIELD, "")).strip())
        for r in branch_raw
    ]

    db_rows = db.query(
        "SELECT branch_code, branch_name, broker_code FROM broker_branches",
        max_rows=10000,
    )
    db_branches: Dict[str, str] = {r["branch_code"]: r.get("branch_name") or "" for r in db_rows}
    existing = {r["branch_code"]: r.get("broker_code") for r in db_rows}

    resolved = resolve_broker_mapping(branch_rows, hq_rows, db_branches)

    # Only branches that exist in broker_branches are updated, and only where the value changes.
    updates = [
        (resolved.mapping[code], code, resolved.mapping[code])
        for code in db_branches
        if code in resolved.mapping and existing.get(code) != resolved.mapping[code]
    ]
    brokers = sorted((code, name[:NAME_MAX]) for code, name in resolved.brokers.items())

    plan = Plan()
    if brokers:
        plan.statements.append(Statement(UPSERT_BROKER_SQL, brokers, many=True))
    if updates:
        plan.statements.append(Statement(UPDATE_BRANCH_SQL, updates, many=True))

    counts = resolved.method_counts()
    breakdown = "、".join(
        f"{METHOD_NAMES.get(m, m)} {n:,}"
        for m, n in sorted(counts.items(), key=lambda kv: -kv[1])
    )
    resolvable = sum(1 for code in db_branches if code in resolved.mapping)

    plan.report += [
        "",
        f"brokers 券商總公司：來源 {len(hq_rows):,} 家，將新增或更新 {len(brokers):,} 家",
        f"broker_branches.broker_code：資料庫 {len(db_branches):,} 個分點，"
        f"其中 {resolvable:,} 個可解析、{len(resolved.unresolved_db):,} 個無法解析，"
        f"本次需更新 {len(updates):,} 個",
    ]
    if breakdown:
        plan.report.append(f"對應方式（含來源全部 {len(resolved.methods):,} 筆）：{breakdown}")
    if resolved.unresolved_db:
        plan.report.append("無法解析的分點（保留空值，不猜測）：")
        for code, name in resolved.unresolved_db[:20]:
            plan.report.append(f"- {code} {name}")
        if len(resolved.unresolved_db) > 20:
            plan.report.append(f"...（共 {len(resolved.unresolved_db):,} 個）")
    if resolved.unresolved_source:
        plan.report.append(
            f"來源資料中有 {len(resolved.unresolved_source):,} 筆分公司名稱比不到總公司"
            "（未出現在本資料庫者不影響）"
        )
    return plan


def plan_stocks(client: TWSEAPIClient) -> Plan:
    """Plan the ``stocks`` upsert from the listed+OTC snapshot."""
    # market_snapshot reports a data date per market (they publish a day apart), so render
    # them individually rather than printing the dict.
    snapshot, data_dates = market_snapshot(client)
    if not snapshot:
        raise ReferenceDataUnavailable("無法取得上市／上櫃股票清單")
    dates = "、".join(f"{market} {day or '未知'}" for market, day in data_dates.items()) or "未知"

    rows, skipped = [], 0
    for code, stock in sorted(snapshot.items()):
        market = MARKET_BY_LABEL.get(stock.market)
        if not market or not stock.name:
            skipped += 1
            continue
        rows.append((code, stock.name[:NAME_MAX], market))

    plan = Plan()
    if rows:
        plan.statements.append(Statement(UPSERT_STOCK_SQL, rows, many=True))

    listed = sum(1 for r in rows if r[2] == "TWSE")
    plan.report += [
        "",
        f"stocks 股票主檔：將新增或更新 {len(rows):,} 檔"
        f"（上市 {listed:,} / 上櫃 {len(rows) - listed:,}），資料日 {dates}",
    ]
    if skipped:
        plan.report.append(f"（略過 {skipped:,} 檔缺少名稱或市場別的標的）")
    return plan


def plan(client: TWSEAPIClient, db: BranchDB, target: str = "all") -> Plan:
    """The whole plan for ``target`` ("all", "brokers" or "stocks"). Writes nothing."""
    if target not in ("all", "brokers", "stocks"):
        raise ValueError(f'target 只接受 "all"、"brokers"、"stocks"，收到 {target!r}')
    whole = Plan()
    if target in ("all", "brokers"):
        whole.extend(plan_brokers(client, db))
    if target in ("all", "stocks"):
        whole.extend(plan_stocks(client))
    return whole


def apply(db: BranchDB, statements: Sequence[Statement]) -> List[str]:
    """Run a plan's statements in one transaction; returns one report line each.

    ``brokers`` is written before ``broker_branches`` because ``broker_code`` references it,
    and ``plan`` already emits them in that order. Nothing here touches the trade tables.
    """
    counts = db.write_transaction(statements)
    return [
        f"- {describe(stmt)}：影響 {max(count, 0):,} 列"
        for stmt, count in zip(statements, counts)
    ]
