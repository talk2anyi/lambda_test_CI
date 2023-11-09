"""
test_collator.py
Entry point for testing workflow via Docker

This does not use the lambda docker image to allow for more conventional, straightforward tests
that focus on the collation logic instead of lambda specifics like event triggers
"""
import datetime
import json
import os

import boto3
import pandas as pd
import pytest
from lambda_function import lambda_handler
from parquet import reader

S3_BUCKET = os.getenv("S3_BUCKET")
SESSION = boto3.session.Session()
S3_CLIENT = SESSION.client("s3", endpoint_url=os.getenv("S3_ENDPOINT"))

"""
These test that the collated output matches the already existing
collated logs when the raw upload being processed doesn't contain any changes for 3 log types
to sanity check file read/writes
(contacts are tested below for the update case)
"""


@pytest.mark.integration
def test_base_app_collation_happy_path():
    key = "collated_logs/current/app_packages/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_collation_happy_event.json",
        key,
        "existing_test_logs/app_packages/logs.parquet",
    )
    existing_time = pd.Timestamp("2018-06-05 22:16:45")
    expected_logs_dict = {
        "package_name": ["app.one", "app.two", "app.three"],
        "device_id": ["1", "1", "1"],
        "row_hash": [
            "b5601ac2f9b16be883ccd8ad48f1414c",
            "cea18809ee1d95a3f451fbd4ba6b3a12",
            "3f9030f867fce09ef6a74e346770137a",
        ],
        "id": [
            "3be2883fa0343c5a8b97522cc9625d6d",
            "120a688074dcc1ae043843ed5d5a5394",
            "fb227f4f5dd26282e4661d258fb8608f",
        ],
        "is_deleted": [False, False, False],
        "user_id": [100, 100, 100],
        "ts_updated": [existing_time, existing_time, existing_time],
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    assert new_collated_logs.to_dict(orient="list") == expected_logs.to_dict(
        orient="list"
    )


@pytest.mark.integration
def test_base_sms_collation_happy_path():
    key = "collated_logs/current/sms_log/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_sms_collation_happy_event.json",
        key,
        "existing_test_logs/sms_log/logs.parquet",
    )
    existing_time = pd.Timestamp("2018-06-05 22:16:45")
    expected_logs_dict = {
        "message_body": [
            "Test".encode("utf-8"),
            "Jambo".encode("utf-8"),
            "Jambo Again".encode("utf-8"),
        ],
        "thread_id": [33, 33, 32],
        "sms_type": ["queued", "draft", "all"],
        "contact_id": [0, 0, 0],
        "datetime": [existing_time, existing_time, existing_time],
        "sms_address": ["25575 4026968", "255 540269 68", "+07 54026968"],
        "normalized_sms_address": ["255754026968", "25554026968", "0754026968"],
        "item_id": [124, 123, 122],
        "body_hash": [
            "0cbc6611f5540bd0809a388dc95a615b",
            "5f5d6b63e802282ca4635a7cf1aaad48",
            "f9592a4b13783e81ac15098e1320ebe1",
        ],
        "device_id": ["1", "1", "1"],
        "row_hash": [
            "64b5c0d510250eba624a66f5454f1ed1",
            "b688ae6d7565cd1472abe29b870324f1",
            "2c3aa6c79ddce2dc236d0d0119c228d4",
        ],
        "id": [
            "545ea8f515b3f4e53abf5594b3008a29",
            "b4418ae36ac7433a11582272755983cf",
            "38ebc5c9ee7da69997745d01159abd79",
        ],
        "is_deleted": [False, False, False],
        "user_id": [100, 100, 100],
        "ts_updated": [existing_time, existing_time, existing_time],
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    assert new_collated_logs.to_dict(orient="list") == expected_logs.to_dict(
        orient="list"
    )


@pytest.mark.integration
def test_base_app_collation_gzip_happy_path():
    key = "collated_logs/current/app_packages/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_collation_gzip_happy_event.json",
        key,
        "existing_test_logs/app_packages/logs.parquet",
    )
    existing_time = pd.Timestamp("2018-06-05 22:16:45")
    expected_logs_dict = {
        "package_name": ["app.one", "app.two", "app.three"],
        "device_id": ["1", "1", "1"],
        "row_hash": [
            "b5601ac2f9b16be883ccd8ad48f1414c",
            "cea18809ee1d95a3f451fbd4ba6b3a12",
            "3f9030f867fce09ef6a74e346770137a",
        ],
        "id": [
            "3be2883fa0343c5a8b97522cc9625d6d",
            "120a688074dcc1ae043843ed5d5a5394",
            "fb227f4f5dd26282e4661d258fb8608f",
        ],
        "is_deleted": [False, False, False],
        "user_id": [100, 100, 100],
        "ts_updated": [existing_time, existing_time, existing_time],
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    assert new_collated_logs.to_dict(orient="list") == expected_logs.to_dict(
        orient="list"
    )


@pytest.mark.integration
def test_base_sms_collation_gzip_happy_path():
    key = "collated_logs/current/sms_log/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_sms_collation_gzip_happy_event.json",
        key,
        "existing_test_logs/sms_log/logs.parquet",
    )
    existing_time = pd.Timestamp("2018-06-05 22:16:45")
    expected_logs_dict = {
        "message_body": [
            "Test".encode("utf-8"),
            "Jambo".encode("utf-8"),
            "Jambo Again".encode("utf-8"),
        ],
        "thread_id": [33, 33, 32],
        "sms_type": ["queued", "draft", "all"],
        "contact_id": [0, 0, 0],
        "datetime": [existing_time, existing_time, existing_time],
        "sms_address": ["25575 4026968", "255 540269 68", "+07 54026968"],
        "normalized_sms_address": ["255754026968", "25554026968", "0754026968"],
        "item_id": [124, 123, 122],
        "body_hash": [
            "0cbc6611f5540bd0809a388dc95a615b",
            "5f5d6b63e802282ca4635a7cf1aaad48",
            "f9592a4b13783e81ac15098e1320ebe1",
        ],
        "device_id": ["1", "1", "1"],
        "row_hash": [
            "64b5c0d510250eba624a66f5454f1ed1",
            "b688ae6d7565cd1472abe29b870324f1",
            "2c3aa6c79ddce2dc236d0d0119c228d4",
        ],
        "id": [
            "545ea8f515b3f4e53abf5594b3008a29",
            "b4418ae36ac7433a11582272755983cf",
            "38ebc5c9ee7da69997745d01159abd79",
        ],
        "is_deleted": [False, False, False],
        "user_id": [100, 100, 100],
        "ts_updated": [existing_time, existing_time, existing_time],
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    assert new_collated_logs.to_dict(orient="list") == expected_logs.to_dict(
        orient="list"
    )


@pytest.mark.integration
def test_base_sms_collation_type_field_happy_path():
    key = "collated_logs/current/sms_log/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_sms_collation_type_field_happy_event.json",
        key,
        "existing_test_logs/sms_log/logs.parquet",
    )
    existing_time = pd.Timestamp("2018-06-05 22:16:45")
    expected_logs_dict = {
        "message_body": [
            "Test".encode("utf-8"),
            "Jambo".encode("utf-8"),
            "Jambo Again".encode("utf-8"),
        ],
        "thread_id": [33, 33, 32],
        "sms_type": ["queued", "draft", "all"],
        "contact_id": [0, 0, 0],
        "datetime": [existing_time, existing_time, existing_time],
        "sms_address": ["25575 4026968", "255 540269 68", "+07 54026968"],
        "normalized_sms_address": ["255754026968", "25554026968", "0754026968"],
        "item_id": [124, 123, 122],
        "body_hash": [
            "0cbc6611f5540bd0809a388dc95a615b",
            "5f5d6b63e802282ca4635a7cf1aaad48",
            "f9592a4b13783e81ac15098e1320ebe1",
        ],
        "device_id": ["1", "1", "1"],
        "row_hash": [
            "64b5c0d510250eba624a66f5454f1ed1",
            "b688ae6d7565cd1472abe29b870324f1",
            "2c3aa6c79ddce2dc236d0d0119c228d4",
        ],
        "id": [
            "545ea8f515b3f4e53abf5594b3008a29",
            "b4418ae36ac7433a11582272755983cf",
            "38ebc5c9ee7da69997745d01159abd79",
        ],
        "is_deleted": [False, False, False],
        "user_id": [100, 100, 100],
        "ts_updated": [existing_time, existing_time, existing_time],
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    assert new_collated_logs.to_dict(orient="list") == expected_logs.to_dict(
        orient="list"
    )


@pytest.mark.integration
def test_base_sms_collation_sad_path():
    key = "collated_logs/current/sms_log/user=100/logs.parquet"
    # expect pytest to raise JSONDecodeError because the file doesn't contain json
    with pytest.raises(json.decoder.JSONDecodeError):
        _ = _run_collation_with_reset(
            "test_events/test_sms_collation_sad_event.json",
            key,
            "existing_test_logs/sms_log/logs.parquet",
        )


"""
These test various scenarios like updates, deletes, and additions
"""


# This tests that the collated output includes new log entries
# when the raw upload being processed contains new entries
@pytest.mark.integration
def test_base_collation_additive_entries():
    key = "collated_logs/current/app_packages/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_collation_addition_event.json",
        key,
        "existing_test_logs/app_packages/logs.parquet",
    )
    expected_logs_dict = {
        "package_name": ["app.one", "app.two", "app.three", "app.four", "app.five"],
        "device_id": ["1", "1", "1", "1", "1"],
        "row_hash": [
            "b5601ac2f9b16be883ccd8ad48f1414c",
            "cea18809ee1d95a3f451fbd4ba6b3a12",
            "3f9030f867fce09ef6a74e346770137a",
            "6662b0ed6dd410ee25292009beaa0819",
            "c7afbdc2a0f05d628980644609f55c36",
        ],
        "id": [
            "3be2883fa0343c5a8b97522cc9625d6d",
            "120a688074dcc1ae043843ed5d5a5394",
            "fb227f4f5dd26282e4661d258fb8608f",
            "c74feb6ee54a7e75c687226a3843b8d9",
            "b6df16be7af2d1cf8f5cfcf738ad04fa",
        ],
        "is_deleted": [False, False, False, False, False],
        "user_id": [100, 100, 100, 100, 100],
        # ts_updated is not compared since this will change with each addition
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    new_logs = new_collated_logs.to_dict(orient="list")
    del new_logs["ts_updated"]
    assert new_logs == expected_logs.to_dict(orient="list")


# This tests that the collated output includes new entries for updates
# when the raw upload being processed contains old entries with new row hashes
@pytest.mark.integration
def test_base_contact_collation_updated_entries():
    now = datetime.datetime.now()
    diff_key = (
        "collated_logs/diff/contact_list/ts_update={}/user=100/logs.parquet".format(
            datetime.datetime(now.year, now.month, now.day).strftime("%Y-%m-%d")
        )
    )
    S3_CLIENT.delete_object(Bucket=S3_BUCKET, Key=diff_key)
    key = "collated_logs/current/contact_list/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_collation_update_event.json",
        key,
        "existing_test_logs/contact_list/logs.parquet",
    )
    new_diff_logs = _read_key(diff_key)
    time = pd.Timestamp("2018-06-05 22:16:45")
    expected_logs_dict = {
        "display_name": ["Bob", "Scott", "Liz", "Fred"],
        "item_id": [1, 2, 3, 2],
        "last_time_contacted": [time, time, time, time],
        "photo_id": ["1", "2", "3", "2"],
        "times_contacted": [45, 66, 678, 66],
        "phone_numbers": [
            """[{"item_id":5,"normalized_phone_number":"+254703305009","phone_number":"0703305009"}]""",
            """[{"item_id":6,"normalized_phone_number":"+254703305009","phone_number":"0703305009"}]""",
            """[{"item_id":7,"normalized_phone_number":"+254703305009","phone_number":"0703305009"}]""",
            """[{"item_id": 6, "normalized_phone_number": "+254723270125", "phone_number": "0723 270125"}]""",
        ],
        "device_id": ["1", "1", "1", "1"],
        "row_hash": [
            "e00d3ecf8c2d50b0d0558fd32ca69e41",
            "495f505aee254f2a31812d699be6b0ec",
            "2108b51b72fbc7d4a76172712ed1f97c",
            "29fbdd5b7026edabb693fd0c2491584b",
        ],
        "id": [
            "ce19ed2fc0b21f69bd8948cf1cab4ca4",
            "c086d7b593ac95217ff38ce045bf7992",
            "4532a234f5e5d487f8997ccc8f0c92e7",
            "c086d7b593ac95217ff38ce045bf7992",
        ],
        "is_deleted": [False, False, False, False],
        "user_id": [100, 100, 100, 100],
        # ts_updated is not compared since this will change with each addition
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    new_logs = new_collated_logs.to_dict(orient="list")
    del new_logs["ts_updated"]
    assert new_logs == expected_logs.to_dict(orient="list")
    expected_diff_logs_dict = {
        "display_name": ["Fred"],
        "item_id": [2],
        "last_time_contacted": [time],
        "photo_id": ["2"],
        "times_contacted": [66],
        "phone_numbers": [
            """[{"item_id": 6, "normalized_phone_number": "+254723270125", "phone_number": "0723 270125"}]"""
        ],
        "device_id": ["1"],
        "row_hash": ["29fbdd5b7026edabb693fd0c2491584b"],
        "id": ["c086d7b593ac95217ff38ce045bf7992"],
        "is_deleted": [False],
        "user_id": [100],
        # ts_updated is not compared since this will change with each addition
    }
    expected_diff_logs = pd.DataFrame(expected_diff_logs_dict)
    new_diff_logs = new_diff_logs.to_dict(orient="list")
    del new_diff_logs["ts_updated"]
    assert new_diff_logs == expected_diff_logs.to_dict(orient="list")


# This tests that the collated output includes new entries for deletions
# when the raw upload being processed does not contain all currently existing entries
@pytest.mark.integration
def test_base_collation_deleted_entries():
    key = "collated_logs/current/app_packages/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_collation_deletion_event.json",
        key,
        "existing_test_logs/app_packages/logs.parquet",
    )
    expected_logs_dict = {
        "package_name": ["app.one", "app.two", "app.three", "app.two"],
        "device_id": ["1", "1", "1", "1"],
        "row_hash": [
            "b5601ac2f9b16be883ccd8ad48f1414c",
            "cea18809ee1d95a3f451fbd4ba6b3a12",
            "3f9030f867fce09ef6a74e346770137a",
            "5125ab60b95320be572260b49db0e936",
        ],
        "id": [
            "3be2883fa0343c5a8b97522cc9625d6d",
            "120a688074dcc1ae043843ed5d5a5394",
            "fb227f4f5dd26282e4661d258fb8608f",
            "120a688074dcc1ae043843ed5d5a5394",
        ],
        "is_deleted": [False, False, False, True],
        "user_id": [100, 100, 100, 100],
        # ts_updated is not compared since this will change with each addition
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    new_logs = new_collated_logs.to_dict(orient="list")
    del new_logs["ts_updated"]
    assert new_logs == expected_logs.to_dict(orient="list")


# Test to ensure that a new file is successfully created containing all raw
# entries when this is the first log seen for a given user
@pytest.mark.integration
def test_base_collation_new_user():
    key = "collated_logs/current/app_packages/user=101/logs.parquet"
    new_collated_logs = _run_collation(
        "test_events/test_collation_new_user_event.json", key
    )
    expected_logs_dict = {
        "package_name": ["app.one"],
        "device_id": ["1"],
        "row_hash": ["6e4be4aa6f9217405c6f111b20f51f23"],
        "id": ["2877aebb8ed52790eee2a0d9c47e4578"],
        "is_deleted": [False],
        "user_id": [101],
        # ts_updated is not compared since this will change with each addition
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    new_logs = new_collated_logs.to_dict(orient="list")
    del new_logs["ts_updated"]
    assert new_logs == expected_logs.to_dict(orient="list")


# Test to ensure that new entries are created when a user uploads logs
# from a new device
@pytest.mark.integration
def test_base_collation_same_user_new_device():
    key = "collated_logs/current/app_packages/user=100/logs.parquet"
    new_collated_logs = _run_collation_with_reset(
        "test_events/test_collation_new_device_event.json",
        key,
        "existing_test_logs/app_packages/logs.parquet",
    )
    expected_logs_dict = {
        "package_name": ["app.one", "app.two", "app.three", "app.three"],
        "device_id": ["1", "1", "1", "2"],
        "row_hash": [
            "b5601ac2f9b16be883ccd8ad48f1414c",
            "cea18809ee1d95a3f451fbd4ba6b3a12",
            "3f9030f867fce09ef6a74e346770137a",
            "8a3b87df85c7061f7d7afbe9d227c697",
        ],
        "id": [
            "3be2883fa0343c5a8b97522cc9625d6d",
            "120a688074dcc1ae043843ed5d5a5394",
            "fb227f4f5dd26282e4661d258fb8608f",
            "42b05bb82442879058f028ebf477bcdb",
        ],
        "is_deleted": [False, False, False, False],
        "user_id": [100, 100, 100, 100],
        # ts_updated is not compared since this will change with each addition
    }
    expected_logs = pd.DataFrame(expected_logs_dict)
    new_logs = new_collated_logs.to_dict(orient="list")
    del new_logs["ts_updated"]
    assert new_logs == expected_logs.to_dict(orient="list")


def _run_collation_with_reset(s3_event, key, existing_key):
    # Reset existing logs from a test dataset
    S3_CLIENT.copy_object(
        Bucket=S3_BUCKET, Key=key, CopySource={"Bucket": S3_BUCKET, "Key": existing_key}
    )
    return _run_collation(s3_event, key)


def _run_collation(s3_event, key):
    # Grab event from json file
    event = json.load(open(s3_event))
    # Call collate with that event
    lambda_handler(event, None)
    return _read_key(key)


def _read_key(key):
    # Read newly created collated log from S3
    result = S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.DataFrame(reader(result["Body"]))
