import pytest

from reconstream.config import Settings, parse_sla_overrides


def test_parse_sla_overrides_normalises_currency_and_ignores_blanks():
    assert parse_sla_overrides(" inr=5, EUR = 3 ,,") == {"INR": 5, "EUR": 3}
    assert parse_sla_overrides("") == {}


@pytest.mark.parametrize("raw", ["INR", "INR=", "INR=five", "INR=0", "INR=-1", "RUPEE=5", "1NR=5"])
def test_parse_sla_overrides_rejects_malformed_values(raw):
    # A typo must fail at startup, not silently fall back to the global SLA.
    with pytest.raises(ValueError, match="bad SLA override"):
        parse_sla_overrides(raw)


def test_settings_from_env_reads_overrides_and_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("RECON_SLA_MINUTES", "2")
    monkeypatch.setenv("RECON_SLA_MINUTES_BY_CURRENCY", "INR=5")
    s = Settings.from_env()
    assert s.sla_for("INR") == 5
    assert s.sla_for("inr") == 5
    assert s.sla_for("USD") == 2
    assert s.sla_for(None) == 2


def test_settings_without_overrides_behave_as_before(monkeypatch):
    monkeypatch.delenv("RECON_SLA_MINUTES_BY_CURRENCY", raising=False)
    s = Settings.from_env()
    assert s.sla_minutes_by_currency == {}
    assert s.sla_for("INR") == s.sla_minutes
