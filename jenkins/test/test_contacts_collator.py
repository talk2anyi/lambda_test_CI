import datetime

from contacts_collator import ContactsCollator

# Contacts-specific tests


def test_contacts_collator_rowhash():
    collated_entry = {
        "id": "123fgh",
        "is_deleted": False,
        "display_name": "Leroy",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722326477 / 1000),
        "times_contacted": 67,
        "photo_id": "56645",
        "phone_numbers": '{num: "+3445435"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "56345efdb7ccf98805fbcc59d3289966"
    )

    # Ensure changes in the entry are reflected in the hash
    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "display_name": "Leroy",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722326477 / 1000),
        "times_contacted": 67,
        "photo_id": "56645",
        "phone_numbers": '{num: "+3445435"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "45c2f39966ecf7c55ca37ceaaf1c0666"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "display_name": "Dave",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722326477 / 1000),
        "times_contacted": 67,
        "photo_id": "56645",
        "phone_numbers": '{num: "+3445435"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "8924614ec9d3dfa74322591b6174a376"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "display_name": "Dave",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722366477 / 1000),
        "times_contacted": 67,
        "photo_id": "56645",
        "phone_numbers": '{num: "+3445435"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "739c6a07fd411054fe41dc0c55291bf9"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "display_name": "Dave",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722366477 / 1000),
        "times_contacted": 12,
        "photo_id": "56645",
        "phone_numbers": '{num: "+3445435"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "1a1a7cfb735ec131b289576031f32fcc"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "display_name": "Dave",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722366477 / 1000),
        "times_contacted": 12,
        "photo_id": "56778",
        "phone_numbers": '{num: "+3445435"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "034e499750b1b0bf3229f29ab6d23216"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "display_name": "Dave",
        "last_time_contacted": datetime.datetime.fromtimestamp(1487722366477 / 1000),
        "times_contacted": 12,
        "photo_id": "56778",
        "phone_numbers": '{num: "+3445435", num: "3453455"}',
    }
    assert (
        ContactsCollator.compute_row_hash(collated_entry)
        == "bb95d8858e1c629178820780603af255"
    )


def test_contact_collator_happy_path():
    raw_entry = {
        "display_name": "Deno",
        "item_id": 201338,
        "last_time_contacted": 1510590105792,
        "phone_numbers": [
            {
                "item_id": 417151,
                "normalized_phone_number": "+254729477015",
                "phone_number": "(072) 947-7015",
            },
            {
                "item_id": 417166,
                "normalized_phone_number": "+254729477015",
                "phone_number": "0729477015",
            },
        ],
        "photo_id": "417143",
        "times_contacted": 1,
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    ContactsCollator.collate_entry(collated_entry, raw_entry)
    # Relevant fields are passed along and cleaned
    assert collated_entry["display_name"] == "Deno"
    assert collated_entry["last_time_contacted"] == datetime.datetime(
        2017, 11, 13, 16, 21, 45, 792000
    )
    assert collated_entry["item_id"] == 201338
    assert collated_entry["photo_id"] == "417143"
    assert collated_entry["times_contacted"] == 1
    assert (
        collated_entry["phone_numbers"]
        == """[{"item_id": 417151, "normalized_phone_number": "+254729477015", "phone_number": "(072) 947-7015"}, {"item_id": 417166, "normalized_phone_number": "+254729477015", "phone_number": "0729477015"}]"""
    )
    assert collated_entry["id"] == "b3577e0d98aab314bacb05cfb225b92d"


def test_contact_collator_mising_photo_id():
    raw_entry = {
        "display_name": "Deno",
        "item_id": 201338,
        "last_time_contacted": 1510590105792,
        "phone_numbers": [
            {
                "item_id": 417151,
                "normalized_phone_number": "+254729477015",
                "phone_number": "(072) 947-7015",
            },
            {
                "item_id": 417166,
                "normalized_phone_number": "+254729477015",
                "phone_number": "0729477015",
            },
        ],
        "times_contacted": 1,
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    ContactsCollator.collate_entry(collated_entry, raw_entry)
    assert collated_entry["photo_id"] is None
