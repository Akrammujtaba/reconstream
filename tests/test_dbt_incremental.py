"""fct_recon_exceptions is incremental: a second dbt run picks up only what changed.

Runs the real dbt project against DuckDB (the CI target), changes the gold
fixture table the way the Spark MERGE would, and runs dbt again. Needs dbt-core
and dbt-duckdb, so it is skipped in the plain unit-test job and run by the dbt
job in CI.
"""

import shutil
from pathlib import Path

import pytest

dbt_main = pytest.importorskip("dbt.cli.main")
duckdb = pytest.importorskip("duckdb")

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
MODEL = "fct_recon_exceptions"


@pytest.fixture()
def project(tmp_path, monkeypatch):
    """A throwaway copy of the dbt project with its own DuckDB file."""
    proj = tmp_path / "dbt"
    shutil.copytree(DBT_DIR, proj, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb"))
    db = tmp_path / "ci.duckdb"
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db))
    return proj, db


def dbt(proj, *args):
    res = dbt_main.dbtRunner().invoke(
        [*args, "--project-dir", str(proj), "--profiles-dir", str(proj), "--target", "ci"]
    )
    assert res.success, f"dbt {' '.join(args)} failed: {res.exception}"


def sql(db, query, params=None):
    # Short-lived connections: DuckDB allows one writer per file, and dbt needs it next.
    with duckdb.connect(str(db)) as con:
        return con.execute(query, params or []).fetchall()


def mart(db):
    rows = sql(
        db,
        f"select exception_id, status, resolved_at, payment_ids, amount_at_risk_cents, severity "
        f"from marts.{MODEL} order by exception_id",
    )
    return {r[0]: r[1:] for r in rows}


def test_second_run_applies_new_and_changed_exceptions(project):
    proj, db = project
    dbt(proj, "seed")
    dbt(proj, "run", "-s", MODEL)
    assert set(mart(db)) == {"x1", "x2", "x3", "x4", "x5"}

    # Newest last_evaluated_at in the fixture is 2026-01-02 03:00:10 (x5), so the
    # default 60-minute lookback re-reads rows evaluated from 02:00:10 onward.
    gold = "gold.recon_exceptions"
    # The stream resolves x2: the MERGE moves last_evaluated_at forward.
    sql(db, f"update {gold} set status = 'RESOLVED', resolved_at = timestamp '2026-01-02 03:05:00', "
            f"last_evaluated_at = timestamp '2026-01-02 03:05:00' where exception_id = 'x2'")
    # A brand-new exception.
    sql(db, f"insert into {gold} select 'x6', 'bk_new', 'AMOUNT_MISMATCH', 'OPEN', 5000, 4000, 1000, "
            f"'USD', 'USD', 'pay_20', timestamp '2026-01-02 03:01:00', timestamp '2026-01-02 03:06:00', "
            f"timestamp '2026-01-02 03:06:00', timestamp '2026-01-02 03:06:00', null, 'stream', 0")
    # A late committer: stamped before the current max, committed after it.
    sql(db, f"update {gold} set payment_ids = 'pay_2,pay_3,pay_4', "
            f"last_evaluated_at = timestamp '2026-01-02 02:30:00' where exception_id = 'x1'")

    dbt(proj, "run", "-s", MODEL)
    after = mart(db)

    assert set(after) == {"x1", "x2", "x3", "x4", "x5", "x6"}
    assert after["x2"][0] == "RESOLVED"
    assert str(after["x2"][1]) == "2026-01-02 03:05:00"
    assert after["x6"][4] == "high"  # joined to the exception_types seed
    assert after["x1"][2] == "pay_2,pay_3,pay_4", "lookback must cover late-committing writers"
    # No duplicates from delete+insert, and the mart's own tests still hold.
    assert sql(db, f"select count(*), count(distinct exception_id) from marts.{MODEL}") == [(6, 6)]
    dbt(proj, "test", "-s", MODEL)


def test_rows_outside_the_lookback_are_not_reread_until_full_refresh(project):
    """Proves the run is really incremental, and shows when --full-refresh is needed."""
    proj, db = project
    dbt(proj, "seed")
    dbt(proj, "run", "-s", MODEL)

    # Change x4 without moving last_evaluated_at (2026-01-01 10:10, far outside the
    # window). The gold MERGE never does this; it stands in for a seed edit, which
    # also changes output without touching last_evaluated_at.
    sql(db, "update gold.recon_exceptions set amount_at_risk_cents = 1 where exception_id = 'x4'")

    dbt(proj, "run", "-s", MODEL)
    assert mart(db)["x4"][3] == 15000

    dbt(proj, "run", "-s", MODEL, "--full-refresh")
    assert mart(db)["x4"][3] == 1


def test_lookback_is_configurable(project):
    proj, db = project
    dbt(proj, "seed")
    dbt(proj, "run", "-s", MODEL)

    # Same late commit as above, but with a 10-minute lookback it falls outside.
    sql(db, "update gold.recon_exceptions set payment_ids = 'pay_2,pay_3,pay_4', "
            "last_evaluated_at = timestamp '2026-01-02 02:30:00' where exception_id = 'x1'")
    dbt(proj, "run", "-s", MODEL, "--vars", "{recon_exceptions_lookback_minutes: 10}")
    assert mart(db)["x1"][2] == "pay_2,pay_3"
