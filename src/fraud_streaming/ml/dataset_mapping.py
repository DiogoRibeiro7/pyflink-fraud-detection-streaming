"""Dataset mapping helpers for labelled public or internal evaluation data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CANONICAL_TRANSACTION_FIELDS: tuple[str, ...] = (
    "transaction_id",
    "user_id",
    "card_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "device_id",
    "merchant_category",
    "event_time",
    "channel",
    "is_card_present",
    "latitude",
    "longitude",
)


@dataclass(frozen=True, slots=True)
class DatasetMappingConfig:
    """Mapping from external dataset columns into canonical transaction fields."""

    field_map: dict[str, str]
    defaults: dict[str, Any]
    value_maps: dict[str, dict[str, Any]]


def load_dataset_mapping(path: Path | None) -> DatasetMappingConfig | None:
    """Load a dataset mapping JSON file when one is provided."""
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("dataset mapping JSON must decode to an object")

    field_map = payload.get("field_map", {})
    defaults = payload.get("defaults", {})
    value_maps = payload.get("value_maps", {})
    if not isinstance(field_map, dict):
        raise ValueError("field_map must be an object")
    if not isinstance(defaults, dict):
        raise ValueError("defaults must be an object")
    if not isinstance(value_maps, dict):
        raise ValueError("value_maps must be an object")

    normalized_field_map = {str(key): str(value) for key, value in field_map.items()}
    for canonical_field in normalized_field_map:
        if canonical_field not in CANONICAL_TRANSACTION_FIELDS and canonical_field != "label":
            raise ValueError(f"unsupported canonical field in field_map: {canonical_field}")

    normalized_value_maps: dict[str, dict[str, Any]] = {}
    for field_name, mapping in value_maps.items():
        if field_name not in CANONICAL_TRANSACTION_FIELDS and field_name != "label":
            raise ValueError(f"unsupported field in value_maps: {field_name}")
        if not isinstance(mapping, dict):
            raise ValueError(f"value_maps entry for {field_name} must be an object")
        normalized_value_maps[str(field_name)] = {str(key): value for key, value in mapping.items()}

    return DatasetMappingConfig(
        field_map=normalized_field_map,
        defaults={str(key): value for key, value in defaults.items()},
        value_maps=normalized_value_maps,
    )


def apply_dataset_mapping(
    payload: dict[str, Any],
    mapping: DatasetMappingConfig | None,
) -> dict[str, Any]:
    """Map an external payload into canonical training fields when configured."""
    if mapping is None:
        return dict(payload)

    normalized: dict[str, Any] = dict(mapping.defaults)
    consumed_source_fields: set[str] = set()

    for canonical_field in CANONICAL_TRANSACTION_FIELDS:
        source_field = mapping.field_map.get(canonical_field, canonical_field)
        if source_field in payload:
            normalized[canonical_field] = payload[source_field]
            consumed_source_fields.add(source_field)

    label_source_field = mapping.field_map.get("label", "label")
    if label_source_field in payload:
        normalized["label"] = payload[label_source_field]
        consumed_source_fields.add(label_source_field)

    for field_name, value_map in mapping.value_maps.items():
        if field_name in normalized:
            lookup_key = str(normalized[field_name])
            if lookup_key in value_map:
                normalized[field_name] = value_map[lookup_key]

    for numeric_field in ("amount", "latitude", "longitude"):
        if normalized.get(numeric_field) not in {None, ""}:
            normalized[numeric_field] = float(normalized[numeric_field])
    if "is_card_present" in normalized and not isinstance(normalized["is_card_present"], bool):
        text = str(normalized["is_card_present"]).strip().lower()
        normalized["is_card_present"] = text in {"true", "1", "yes", "y"}

    for key, value in payload.items():
        if key not in consumed_source_fields and key not in normalized:
            normalized[key] = value

    return normalized
