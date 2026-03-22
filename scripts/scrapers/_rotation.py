#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence, TypeVar

T = TypeVar("T")


def load_rotation_state(path: str | Path) -> dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def save_rotation_state(path: str | Path, state: dict[str, Any]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _normalize_limit(limit: int | None, total: int) -> int:
    if total <= 0:
        return 0
    if limit is None or limit <= 0:
        return total
    return min(limit, total)


def select_round_robin_batch(
    items: Sequence[T],
    limit: int | None,
    state: dict[str, Any],
    key: str,
) -> list[T]:
    item_list = list(items)
    total = len(item_list)
    take = _normalize_limit(limit, total)
    if total == 0 or take == 0:
        state[key] = 0
        return []

    offset = int(state.get(key, 0) or 0) % total
    rotated = item_list[offset:] + item_list[:offset]
    selected = rotated[:take]
    state[key] = (offset + take) % total
    return selected


def select_priority_then_round_robin(
    priority_items: Sequence[T],
    secondary_items: Sequence[T],
    limit: int | None,
    state: dict[str, Any],
    priority_key: str,
    secondary_key: str,
) -> list[T]:
    total_available = len(priority_items) + len(secondary_items)
    total_take = _normalize_limit(limit, total_available)
    if total_take == 0:
        state[priority_key] = 0
        state[secondary_key] = 0
        return []

    selected_priority = select_round_robin_batch(priority_items, total_take, state, priority_key)
    remaining = total_take - len(selected_priority)
    selected_secondary = select_round_robin_batch(secondary_items, remaining, state, secondary_key)
    return selected_priority + selected_secondary
