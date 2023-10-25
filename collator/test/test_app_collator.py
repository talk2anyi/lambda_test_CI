from app_collator import AppCollator

# App-specific tests


def test_app_collator_rowhash():
    collated_entry = {"id": "123fgh", "is_deleted": False}
    assert (
        AppCollator.compute_row_hash(collated_entry)
        == "a518d55eec549d7d8a88390e6ee860ef"
    )

    # Ensure changes in the entry are reflected in the hash
    collated_entry = {"id": "123fgh", "is_deleted": True}
    AppCollator.compute_row_hash(collated_entry)
    assert (
        AppCollator.compute_row_hash(collated_entry)
        == "da91ef587b2dddc0dd48912ad3295922"
    )


def test_app_collator_happy_path():
    raw_entry = {"package_name": "App. Name"}
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    AppCollator.collate_entry(collated_entry, raw_entry)
    # Relevant fields are passed along and cleaned
    assert collated_entry["package_name"] == "app.name"
    assert collated_entry["id"] == "8d11212237eb630501664d6574d9c550"


def test_app_collator_empty_package():
    raw_entry_empty = {"package_name": ""}
    raw_entry_none = {"package_name": None}
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    assert not AppCollator.collate_entry(collated_entry, raw_entry_none)
    assert not AppCollator.collate_entry(collated_entry, raw_entry_empty)
