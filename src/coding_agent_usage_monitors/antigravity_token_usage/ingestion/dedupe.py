"""Global deduplication module for AntiGravity usage events."""

from __future__ import annotations

from .schemas import RawUsageEvent, UsageEventRow

SOURCE_PRIORITY: dict[str, int] = {
    "default": 1,
    "cli": 2,
    "ide": 3,
    "backup": 4,
    "config": 5,
    "env": 6,
}


def get_identity_keys(event: RawUsageEvent) -> list[str]:
    """Extract non-empty identity keys for an event in preference order.

    Args:
        event (RawUsageEvent): Candidate raw usage event.

    Returns:
        list[str]: Identity key strings.
    """
    keys: list[str] = []
    if event.response_id and event.response_id.strip():
        keys.append(f"response:{event.response_id}")
    if event.provider_assigned_message_id and event.provider_assigned_message_id.strip():
        keys.append(f"provider:{event.provider_assigned_message_id}")
    if event.message_id and event.message_id.strip():
        keys.append(f"message:{event.message_id}")
    return keys


def merge_events(events: list[RawUsageEvent]) -> UsageEventRow:
    """Merge duplicate events into one canonical UsageEventRow.

    Args:
        events (list[RawUsageEvent]): Non-empty list of duplicate events to merge.

    Returns:
        UsageEventRow: Merged canonical event row.
    """

    inp = max(e.input_tokens for e in events)
    out = max(e.output_tokens for e in events)
    reas = max(e.reasoning_tokens for e in events)
    tot_out = max(e.total_output_tokens for e in events)
    c_create = max(e.cache_creation_tokens for e in events)
    c_read = max(e.cache_read_tokens for e in events)
    total_tokens = inp + c_create + c_read + tot_out

    best_source_event = min(
        events,
        key=lambda e: SOURCE_PRIORITY.get(e.discovery_source, 999),
    )
    winning_source = best_source_event.discovery_source

    max_rank = max(e.timestamp_rank for e in events)
    rank_events = [e for e in events if e.timestamp_rank == max_rank]
    best_ts_event = min(rank_events, key=lambda e: e.event_timestamp)
    winning_ts = best_ts_event.event_timestamp

    non_default_models = [e.model_code for e in events if e.model_code != "gemini-internal-model"]
    winning_model_code = non_default_models[0] if non_default_models else "gemini-internal-model"

    providers = [e.provider_id for e in events if e.provider_id is not None]
    winning_provider_id = providers[0] if providers else None

    response_id = next((e.response_id for e in events if e.response_id and e.response_id.strip()), None)
    provider_assigned_message_id = next(
        (
            e.provider_assigned_message_id
            for e in events
            if e.provider_assigned_message_id and e.provider_assigned_message_id.strip()
        ),
        None,
    )
    message_id = next((e.message_id for e in events if e.message_id and e.message_id.strip()), None)

    if response_id:
        event_key = f"response:{response_id}"
    elif provider_assigned_message_id:
        event_key = f"provider:{provider_assigned_message_id}"
    elif message_id:
        event_key = f"message:{message_id}"
    else:
        event_key = events[0].fallback_key

    session_id = best_source_event.session_id

    return UsageEventRow(
        event_key=event_key,
        discovery_source=winning_source,
        session_id=session_id,
        event_timestamp=winning_ts,
        model_code=winning_model_code,
        provider_id=winning_provider_id,
        input_tokens=inp,
        output_tokens=out,
        reasoning_tokens=reas,
        total_output_tokens=tot_out,
        cache_creation_tokens=c_create,
        cache_read_tokens=c_read,
        total_tokens=total_tokens,
        response_id=response_id,
        provider_assigned_message_id=provider_assigned_message_id,
        message_id=message_id,
    )


def deduplicate_events(events: list[RawUsageEvent]) -> list[UsageEventRow]:
    """Perform global deduplication of candidate raw events.

    Args:
        events (list[RawUsageEvent]): All extracted raw events across discovered databases.

    Returns:
        list[UsageEventRow]: Canonical deduplicated events sorted deterministically by event_key.
    """
    if not events:
        return []

    slots: list[list[RawUsageEvent]] = []
    key_to_slot_idx: dict[str, int] = {}

    for event in events:
        keys = get_identity_keys(event)
        if not keys:
            slots.append([event])
            continue

        matching_indices = sorted({key_to_slot_idx[k] for k in keys if k in key_to_slot_idx})

        if not matching_indices:
            new_idx = len(slots)
            slots.append([event])
            for k in keys:
                key_to_slot_idx[k] = new_idx
        elif len(matching_indices) == 1:
            target_idx = matching_indices[0]
            slots[target_idx].append(event)
            for k in keys:
                key_to_slot_idx[k] = target_idx
        else:
            target_idx = matching_indices[0]
            target_list = slots[target_idx]
            target_list.append(event)
            for other_idx in matching_indices[1:]:
                target_list.extend(slots[other_idx])
                slots[other_idx] = []

            for k, idx_val in list(key_to_slot_idx.items()):
                if idx_val in matching_indices:
                    key_to_slot_idx[k] = target_idx

            for k in keys:
                key_to_slot_idx[k] = target_idx

    canonical_rows: list[UsageEventRow] = []
    for slot_events in slots:
        if slot_events:
            canonical_rows.append(merge_events(slot_events))

    canonical_rows.sort(key=lambda r: r.event_key)
    return canonical_rows
