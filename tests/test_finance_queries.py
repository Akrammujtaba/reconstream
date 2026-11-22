"""The SQL in docs/queries.md runs and gives the right answers.

Docs queries rot quietly: a column gets renamed in a mart and nobody notices
until finance pastes a query that fails. So this test pulls every ```sql block
out of docs/queries.md and runs it, unchanged, against the dbt CI fixtures on
DuckDB, then checks the answers by hand-computed values from the fixture CSVs.

The queries stick to SQL that Trino and DuckDB both accept, which is what makes
running them here meaningful. This checks the SQL and the logic, not Trino.

Needs dbt-core and dbt-duckdb, so it is skipped in the plain unit-test job and
run by the dbt job in CI.
"""

import re
import shutil
from pathlib import Path

import pytest

dbt_main = pytest.importorskip("dbt.cli.main")
duckdb = pytest.importorskip("duckdb")

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "queries.md"

# "## 1. Title" followed (eventually) by a ```sql block.
SECTION = re.compile(r"^## (\d+)\. (.+?)$.*?```sql\n(.*?)```", re.MULTILINE | re.DOTALL)


def doc_queries():
    """{number: sql} for every numbered section in docs/queries.md."""
    return {int(n): sql for n, _title, sql in SECTION.findall(DOC.read_text())}


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    """A DuckDB file with the CI seeds loaded and the marts built, shared by all tests."""
    tmp = tmp_path_factory.mktemp("finance_queries")
    proj = tmp / "dbt"
    shutil.copytree(ROOT / "dbt", proj, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb"))
    path = tmp / "ci.duckdb"
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("DBT_DUCKDB_PATH", str(path))
        for args in (["seed"], ["run", "-s", "fct_recon_exceptions"]):
            res = dbt_main.dbtRunner().invoke(
                [*args, "--project-dir", str(proj), "--profiles-dir", str(proj), "--target", "ci"]
            )
            assert res.success, f"dbt {' '.join(args)} failed: {res.exception}"
    return path


def run(db, n):
    # Same config as dbt's connection: DuckDB refuses a read_only one while dbt's is cached.
    with duckdb.connect(str(db)) as con:
        return con.execute(doc_queries()[n]).fetchall()


def test_doc_has_five_numbered_queries():
    assert sorted(doc_queries()) == [1, 2, 3, 4, 5]


def test_no_integer_division():
    """Trino's `/` on two integers truncates, DuckDB's doesn't. Cast first so both agree."""
    for n, sql in doc_queries().items():
        for line in sql.splitlines():
            if "/" in line:
                assert "as double" in line, f"query {n}: no cast to double in: {line.strip()}"


def test_open_exposure_by_currency(db):
    # Open: x1 EUR 180.00 (x1.08), x2 GBP 90.00 (x1.27), x4 USD 150.00, and
    # x5 USD 70.00 (orphan payment, so the currency comes from the payment).
    assert run(db, 1) == [
        ("USD", 2, 220.0, 220.0),
        ("EUR", 1, 180.0, 194.4),
        ("GBP", 1, 90.0, 114.3),
    ]


def test_slowest_to_resolve(db):
    # Only x3 is resolved: detected 10:05:40, resolved 10:06:40.
    assert run(db, 2) == [("MISSING_PAYMENT", 1, 1.0, 1.0)]


def test_ops_queue_is_severity_then_age(db):
    rows = run(db, 3)
    assert [r[0] for r in rows] == ["x1", "x2", "x4", "x5"]
    assert [(r[3], r[4]) for r in rows] == [
        ("critical", "payments-ops"),
        ("high", "payments-ops"),
        ("high", "finance-ops"),
        ("medium", "finance-ops"),
    ]
    # x2: detected 10:04:45, last evaluated 10:10:00 -> 315 s.
    assert rows[1][7] == 5.3


def test_detection_speed_by_type(db):
    # due_at -> first_detected_at: x1 12 s, x2 44 s, x3 39 s, x4 35 s, x5 10 s.
    assert run(db, 4) == [
        ("MISSING_PAYMENT", 2, 41.5, 44),
        ("REFUND_MISMATCH", 1, 35.0, 35),
        ("DUPLICATE_CHARGE", 1, 12.0, 12),
        ("ORPHAN_PAYMENT", 1, 10.0, 10),
    ]


def test_duplicate_charge_refunds(db):
    rows = run(db, 5)
    assert [r[:6] for r in rows] == [("bk_double", "g2", "pay_2,pay_3", "EUR", 180.0, 194.4)]
