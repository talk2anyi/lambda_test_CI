import datetime

from base_collator import BaseCollator
from sms_collator import SmsCollator

# Sms-specific tests


def test_sms_collator_rowhash():
    collated_entry = {
        "id": "123fgh",
        "is_deleted": False,
        "thread_id": 5,
        "contact_id": 9,
        "sms_type": "sent",
        "normalized_sms_address": "4567895",
    }
    assert (
        SmsCollator.compute_row_hash(collated_entry)
        == "e97347ee50f9a5f1155061c096672498"
    )

    # Ensure changes in the entry are reflected in the hash
    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "thread_id": 5,
        "contact_id": 9,
        "sms_type": "sent",
        "normalized_sms_address": "4567895",
    }
    assert (
        SmsCollator.compute_row_hash(collated_entry)
        == "0d06f894e0a9b414505c9ce87bc65d84"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "thread_id": 8,
        "contact_id": 9,
        "sms_type": "sent",
        "normalized_sms_address": "4567895",
    }
    assert (
        SmsCollator.compute_row_hash(collated_entry)
        == "1979b8a0627138f78b8e5fcc79e014a3"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "thread_id": 8,
        "contact_id": 3,
        "sms_type": "sent",
        "normalized_sms_address": "4567895",
    }
    assert (
        SmsCollator.compute_row_hash(collated_entry)
        == "301be5ea0834622aae3f2db184a9c846"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "thread_id": 8,
        "contact_id": 3,
        "sms_type": "inbox",
        "normalized_sms_address": "4567895",
    }
    assert (
        SmsCollator.compute_row_hash(collated_entry)
        == "cb29ce1ae0059c5d5aac48c9d3a8e647"
    )

    collated_entry = {
        "id": "123fgh",
        "is_deleted": True,
        "thread_id": 8,
        "contact_id": 3,
        "sms_type": "inbox",
        "normalized_sms_address": "3456643",
    }
    assert (
        SmsCollator.compute_row_hash(collated_entry)
        == "41edc13b4ff534ea2ac2def36903018a"
    )


def test_sms_collator_happy_path():
    raw_entry = {
        "contact_id": 0,
        "datetime": 1487722326477,
        "item_id": 122,
        "message_body": "Jambo people",
        "sms_address": "+075 40269 68",
        "sms_type": 6,
        "thread_id": 32,
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    SmsCollator.collate_entry(collated_entry, raw_entry)
    # Relevant fields are passed along and cleaned
    assert collated_entry["message_body"] == b"Jambo people"
    assert collated_entry["sms_type"] == "queued"
    assert collated_entry["item_id"] == 122
    assert collated_entry["thread_id"] == 32
    assert collated_entry["contact_id"] == 0
    assert collated_entry["datetime"] == datetime.datetime(
        2017, 2, 22, 0, 12, 6, 477000
    )
    assert collated_entry["sms_address"] == "+075 40269 68"
    assert collated_entry["normalized_sms_address"] == "0754026968"
    assert collated_entry["body_hash"] == "2e962e16e56aaed080779ea915252cb2"
    assert collated_entry["id"] == "1846170e2c6acd9e66f747148519ec34"


def test_sms_collator_type_field_happy_path():
    """'sms_type' was mistakenly called 'type' in some versions of the app"""
    raw_entry = {
        "contact_id": 0,
        "datetime": 1487722326477,
        "item_id": 122,
        "message_body": "Jambo people",
        "sms_address": "+075 40269 68",
        "type": 6,
        "thread_id": 32,
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    SmsCollator.collate_entry(collated_entry, raw_entry)
    # Relevant fields are passed along and cleaned
    assert collated_entry["message_body"] == b"Jambo people"
    assert collated_entry["sms_type"] == "queued"
    assert collated_entry["item_id"] == 122
    assert collated_entry["thread_id"] == 32
    assert collated_entry["contact_id"] == 0
    assert collated_entry["datetime"] == datetime.datetime(
        2017, 2, 22, 0, 12, 6, 477000
    )
    assert collated_entry["sms_address"] == "+075 40269 68"
    assert collated_entry["normalized_sms_address"] == "0754026968"
    assert collated_entry["body_hash"] == "2e962e16e56aaed080779ea915252cb2"
    assert collated_entry["id"] == "1846170e2c6acd9e66f747148519ec34"


def test_sms_collator_unknown_sms_type():
    """Ensure that an unknown SMS type (13) is replaced with 'unknown'"""
    raw_entry = {
        "contact_id": 0,
        "sms_type": 13,
        "datetime": 1487722326477,
        "item_id": 122,
        "message_body": "Jambo people",
        "sms_address": "+075 40269 68",
        "thread_id": 32,
    }
    collated_entry = {"user_id": "123", "device_id": "456", "is_deleted": False}
    SmsCollator.collate_entry(collated_entry, raw_entry)
    assert collated_entry["sms_type"] == "unknown"


def test_sms_collator_future_timestamps():
    sms_logs = [
        {
            "body_hash": "0cbc6611f5540bd0809a388dc95a615b",
            "contact_id": 0,
            "datetime": datetime.datetime(2018, 6, 5, 22, 16, 45),
            "device_id": "1",
            "id": "545ea8f515b3f4e53abf5594b3008a29",
            "is_deleted": False,
            "item_id": 124,
            "message_body": b"Test",
            "normalized_sms_address": "255754026968",
            "row_hash": "64b5c0d510250eba624a66f5454f1ed1",
            "sms_address": "25575 4026968",
            "sms_type": "queued",
            "thread_id": 33,
            "ts_updated": datetime.datetime(2018, 6, 5, 23, 16, 45),
            "user_id": 100,
        },
        {
            "body_hash": "5f5d6b63e802282ca4635a7cf1aaad48",
            "contact_id": 0,
            "datetime": datetime.datetime(2021, 6, 5, 22, 16, 45),
            "device_id": "1",
            "id": "b4418ae36ac7433a11582272755983cf",
            "is_deleted": False,
            "item_id": 123,
            "message_body": b"Jambo",
            "normalized_sms_address": "25554026968",
            "row_hash": "b688ae6d7565cd1472abe29b870324f1",
            "sms_address": "255 540269 68",
            "sms_type": "draft",
            "thread_id": 33,
            "ts_updated": datetime.datetime(2018, 6, 5, 22, 16, 45),
            "user_id": 100,
        },
        {
            "body_hash": "f9592a4b13783e81ac15098e1320ebe1",
            "contact_id": 0,
            "datetime": datetime.datetime(2022, 6, 5, 22, 16, 45),
            "device_id": "1",
            "id": "38ebc5c9ee7da69997745d01159abd79",
            "is_deleted": False,
            "item_id": 122,
            "message_body": b"Jambo Again",
            "normalized_sms_address": "0754026968",
            "row_hash": "2c3aa6c79ddce2dc236d0d0119c228d4",
            "sms_address": "+07 54026968",
            "sms_type": "all",
            "thread_id": 32,
            "ts_updated": datetime.datetime(2018, 9, 5, 22, 16, 45),
            "user_id": 100,
        },
    ]
    corrected_logs = [
        {
            "body_hash": "0cbc6611f5540bd0809a388dc95a615b",
            "contact_id": 0,
            "datetime": datetime.datetime(2018, 6, 5, 22, 16, 45),
            "device_id": "1",
            "id": "545ea8f515b3f4e53abf5594b3008a29",
            "is_deleted": False,
            "item_id": 124,
            "message_body": b"Test",
            "normalized_sms_address": "255754026968",
            "row_hash": "64b5c0d510250eba624a66f5454f1ed1",
            "sms_address": "25575 4026968",
            "sms_type": "queued",
            "thread_id": 33,
            "ts_updated": datetime.datetime(2018, 6, 5, 23, 16, 45),
            "user_id": 100,
        },
        {
            "body_hash": "5f5d6b63e802282ca4635a7cf1aaad48",
            "contact_id": 0,
            "datetime": datetime.datetime(2018, 6, 5, 22, 16, 45),
            "device_id": "1",
            "id": "b4418ae36ac7433a11582272755983cf",
            "is_deleted": False,
            "item_id": 123,
            "message_body": b"Jambo",
            "normalized_sms_address": "25554026968",
            "row_hash": "b688ae6d7565cd1472abe29b870324f1",
            "sms_address": "255 540269 68",
            "sms_type": "draft",
            "thread_id": 33,
            "ts_updated": datetime.datetime(2018, 6, 5, 22, 16, 45),
            "user_id": 100,
        },
        {
            "body_hash": "f9592a4b13783e81ac15098e1320ebe1",
            "contact_id": 0,
            "datetime": datetime.datetime(2018, 9, 5, 22, 16, 45),
            "device_id": "1",
            "id": "38ebc5c9ee7da69997745d01159abd79",
            "is_deleted": False,
            "item_id": 122,
            "message_body": b"Jambo Again",
            "normalized_sms_address": "0754026968",
            "row_hash": "2c3aa6c79ddce2dc236d0d0119c228d4",
            "sms_address": "+07 54026968",
            "sms_type": "all",
            "thread_id": 32,
            "ts_updated": datetime.datetime(2018, 9, 5, 22, 16, 45),
            "user_id": 100,
        },
    ]
    assert BaseCollator.future_timestamp_handler(sms_logs) == corrected_logs
