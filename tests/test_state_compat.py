from __future__ import annotations

import json
from pathlib import Path

import pytest

from fraud_streaming.schemas import UserProfileState

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "state"


def test_user_profile_state_reads_legacy_fixture_without_schema_version() -> None:
    payload = json.loads((FIXTURE_DIR / "user_profile_state_v1.json").read_text(encoding="utf-8"))

    state = UserProfileState.from_dict(payload)

    assert state.count == 3
    assert state.last_country == "PT"
    assert len(state.rolling_transactions) == 2


def test_user_profile_state_round_trips_current_fixture() -> None:
    payload = json.loads((FIXTURE_DIR / "user_profile_state_v2.json").read_text(encoding="utf-8"))

    state = UserProfileState.from_dict(payload)

    assert state.to_dict()["schema_version"] == UserProfileState.SCHEMA_VERSION
    assert state.last_device_id == "device-9"


def test_user_profile_state_rejects_unknown_schema_version() -> None:
    with pytest.raises(ValueError, match="unsupported state schema_version"):
        UserProfileState.from_dict({"schema_version": 999})
