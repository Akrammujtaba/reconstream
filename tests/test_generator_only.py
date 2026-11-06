"""`run.py --only`: the simulator injects one fault type and keeps the noise."""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
from simulator import DEFAULT_FAULT_RATES, FAULTS, Config, Simulator, only_fault  # noqa: E402

START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


class FakeSink:
    def __init__(self):
        self.faults = []

    def insert_booking(self, row):
        pass

    def update_booking(self, booking_id, changes):
        pass

    def deliver(self, event):
        pass

    def record_fault(self, fault):
        self.faults.append(fault)


def simulate(cfg, minutes=10):
    sink = FakeSink()
    sim = Simulator(sink, cfg, seed=11)
    now = START
    while now < START + timedelta(minutes=minutes):
        sim.tick(now)
        now += timedelta(milliseconds=500)
    sim.drain()
    return sim, sink


def test_only_keeps_the_chosen_rate_and_zeroes_the_rest():
    cfg = only_fault(Config(), "AMOUNT_MISMATCH")
    assert cfg.fault_rates["AMOUNT_MISMATCH"] == DEFAULT_FAULT_RATES["AMOUNT_MISMATCH"]
    assert all(v == 0 for k, v in cfg.fault_rates.items() if k != "AMOUNT_MISMATCH")
    assert cfg.orphan_rate == 0


def test_only_orphan_keeps_orphan_rate_and_zeroes_fault_rates():
    base = Config()
    cfg = only_fault(base, "ORPHAN_PAYMENT")
    assert cfg.orphan_rate == base.orphan_rate
    assert all(v == 0 for v in cfg.fault_rates.values())


def test_only_does_not_touch_noise_or_the_original_config():
    base = Config()
    cfg = only_fault(base, "DUPLICATE_CHARGE")
    assert cfg.duplicate_delivery_rate == base.duplicate_delivery_rate
    assert cfg.malformed_rate == base.malformed_rate
    assert base.fault_rates == DEFAULT_FAULT_RATES  # copy, not mutated in place


def test_only_rejects_unknown_fault_type():
    with pytest.raises(ValueError, match="unknown fault type"):
        only_fault(Config(), "TYPO_CHARGE")


@pytest.mark.parametrize("fault", sorted(FAULTS))
def test_simulated_run_injects_only_the_chosen_type(fault):
    cfg = only_fault(Config(bookings_per_second=4), fault)
    # Scale the one rate up, as --fault-scale would, so a short run has several.
    cfg.fault_rates = {k: v * 5 for k, v in cfg.fault_rates.items()}
    cfg.orphan_rate *= 5
    sim, sink = simulate(cfg)
    assert len(sink.faults) >= 3
    assert {f["fault_type"] for f in sink.faults} == {fault}
    assert sim.counts["delivery:duplicate"] > 0  # noise still flows
