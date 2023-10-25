import datetime

from call_collator import CallCollator

# Call-specific tests


def test_call_collator_rowhash():
    collated_entry = {
        "id": "123fgh",
        "is_deleted": False,
        "cached_name": "name",
        "call_type": "5",
        "normalized_phone_number": "4567895",
        "duration": 456,
    }
    assert (
        CallCollator.compute_row_hash(collated_entry)
        == "83bafc501b555dfca04a3d37ca5eb573"
    )

    # Ensure changes in the entry are reflected in the hash
    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "cached_name": "name",
        "call_type": "5",
        "normalized_phone_number": "4567895",
        "duration": 456,
    }
    assert (
        CallCollator.compute_row_hash(collated_entry)
        == "b2e2fd82df0d78c5b1926af61edae852"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "cached_name": "new",
        "call_type": "5",
        "normalized_phone_number": "4567895",
        "duration": 456,
    }
    assert (
        CallCollator.compute_row_hash(collated_entry)
        == "5d4caafb13b524293b13af0991f50da3"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "cached_name": "new",
        "call_type": "5",
        "normalized_phone_number": "4567895",
        "duration": 456,
    }
    assert (
        CallCollator.compute_row_hash(collated_entry)
        == "5d4caafb13b524293b13af0991f50da3"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "cached_name": "new",
        "call_type": "5",
        "normalized_phone_number": "1234567",
        "duration": 456,
    }
    assert (
        CallCollator.compute_row_hash(collated_entry)
        == "346c34952b333f23d63d5a6eb8e70ac4"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "cached_name": "new",
        "call_type": "5",
        "normalized_phone_number": "1234567",
        "duration": 789,
    }
    assert (
        CallCollator.compute_row_hash(collated_entry)
        == "346867fa6cc4e884dd08246e9b9347ab"
    )


def test_call_collator_happy_path():
    raw_entry = {
        "cached_name": "test",
        "call_type": "5",
        "datetime": "1466176793178",
        "duration": "15",
        "item_id": 74,
        "phone_number": "+0724 417 503",
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    CallCollator.collate_entry(collated_entry, raw_entry)
    # Relevant fields are passed along and cleaned
    assert collated_entry["cached_name"] == "test"
    assert collated_entry["call_type"] == "rejected"
    assert collated_entry["item_id"] == 74
    assert collated_entry["phone_number"] == "+0724 417 503"
    assert collated_entry["normalized_phone_number"] == "0724417503"
    assert collated_entry["datetime"] == datetime.datetime(
        2016, 6, 17, 15, 19, 53, 178000
    )
    assert collated_entry["duration"] == 15
    assert collated_entry["id"] == "5e7cfe8e771f922f540fb66d159de7ef"


def test_call_collator_missing_call_type():
    raw_entry = {
        "cached_name": "test",
        "call_type": "20",
        "datetime": "1466176793178",
        "duration": "15",
        "item_id": 74,
        "phone_number": "+0724 417 503",
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    CallCollator.collate_entry(collated_entry, raw_entry)
    assert collated_entry["call_type"] == "unknown"


def test_call_collator_missing_cached_name():
    raw_entry = {
        "call_type": "5",
        "datetime": "1466176793178",
        "duration": "15",
        "item_id": 74,
        "phone_number": "+0724 417 503",
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    CallCollator.collate_entry(collated_entry, raw_entry)
    assert collated_entry["cached_name"] is None


def test_call_collator_empty_phone_number():
    raw_entry_empty = {"phone_number": ""}
    raw_entry_none = {"phone_number": None}
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    assert not CallCollator.collate_entry(collated_entry, raw_entry_none)
    assert not CallCollator.collate_entry(collated_entry, raw_entry_empty)
