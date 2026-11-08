"""Metrics snapshot and the Grafana dashboard that reads it."""

from __future__ import annotations

import json
import re
from pathlib import Path

from reconstream.observability import StreamMetrics
from reconstream.stream_processor import changes_by_type

DASHBOARD = Path(__file__).resolve().parents[1] / "infra/grafana/dashboards/reconstream.json"


def row(change, alert, etype, count):
    return {"change": change, "alert": alert, "exception_type": etype, "count": count}


def test_changes_by_type_counts_only_opens_and_resolves():
    stats = [
        row("UPSERT", True, "MISSING_PAYMENT", 3),    # new or re-opened
        row("UPSERT", False, "MISSING_PAYMENT", 7),   # still open: not a change
        row("RESOLVE", False, "MISSING_PAYMENT", 2),
        row("UPSERT", True, "DUPLICATE_CHARGE", 1),
    ]
    assert changes_by_type(stats) == {
        ("MISSING_PAYMENT", "opened"): 3,
        ("MISSING_PAYMENT", "resolved"): 2,
        ("DUPLICATE_CHARGE", "opened"): 1,
    }
    assert changes_by_type([]) == {}


def test_per_type_changes_accumulate_across_batches():
    m = StreamMetrics(None)
    m.push({}, {}, {("MISSING_PAYMENT", "opened"): 2})
    m.push({}, {}, {("MISSING_PAYMENT", "opened"): 1, ("MISSING_PAYMENT", "resolved"): 3})
    m.push({}, {})  # a batch with no reconciliation passes nothing

    reg = m.registry({}, {})
    value = lambda change: reg.get_sample_value(  # noqa: E731
        "reconstream_exception_changes_total",
        {"exception_type": "MISSING_PAYMENT", "change": change},
    )
    assert value("opened") == 3
    assert value("resolved") == 3


def test_every_metric_on_the_dashboard_is_emitted():
    """A renamed or misspelled metric would leave a panel silently empty."""
    m = StreamMetrics(None)
    m.push({}, {"MISSING_PAYMENT": (1, 100)}, {("MISSING_PAYMENT", "opened"): 1})
    emitted = {fam.name for fam in m.registry({}, {"MISSING_PAYMENT": (1, 100)}).collect()}
    # prometheus_client strips the _total suffix from family names; samples keep it.
    emitted |= {f"{name}_total" for name in emitted}

    dashboard = json.loads(DASHBOARD.read_text())
    used = {
        name
        for panel in dashboard["panels"]
        for target in panel["targets"]
        for name in re.findall(r"\breconstream_\w+", target["expr"])
    }
    assert used, "dashboard should query reconstream metrics"
    assert used <= emitted, f"not emitted: {sorted(used - emitted)}"


def test_dashboard_panels_have_unique_ids_and_do_not_overlap():
    panels = json.loads(DASHBOARD.read_text())["panels"]
    ids = [p["id"] for p in panels]
    assert len(ids) == len(set(ids))

    cells = set()
    for p in panels:
        g = p["gridPos"]
        assert g["x"] + g["w"] <= 24, f"panel {p['id']} is wider than the 24-column grid"
        for x in range(g["x"], g["x"] + g["w"]):
            for y in range(g["y"], g["y"] + g["h"]):
                assert (x, y) not in cells, f"panel {p['id']} overlaps another panel"
                cells.add((x, y))
