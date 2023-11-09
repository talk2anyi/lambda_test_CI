"""Sms log-specific logic for collation. Contains knowledge of how to compute
the unique and row hashes for sms log entries, as well as some information about the sms
schema
"""

import hashlib
import json
import logging
import re

from base_collator import BaseCollator
from ddtrace import patch

patch(logging=True)

FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d]"
    " [dd.service=%(dd.service)s dd.env=%(dd.env)s dd.version=%(dd.version)s"
    " dd.trace_id=%(dd.trace_id)s dd.span_id=%(dd.span_id)s] - %(message)s"
)

logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class SmsCollator(BaseCollator):
    SCHEMA_RAW = [
        "message_body",  # str
        "thread_id",  # int
        "sms_type",  # str
        "type",  # str, 2023-09-08: add type field temporarily due to Android bug
        "contact_id",  # int
        "datetime",  # timestamp
        "sms_address",  # str
        "normalized_sms_address",  # str
        "item_id",  # int
        "body_hash",  # str
    ]

    SCHEMA = SCHEMA_RAW + BaseCollator.BASE_SCHEMA

    REQUIRED_FIELDS_TXT = [
        "contact_id",
        "datetime",
        "item_id",
        "message_body",
        "sms_address",
        "sms_type",
        "thread_id",
    ]

    # SMS message type mapping as given by Android
    # https://developer.android.com/reference/android/provider/Telephony.TextBasedSmsColumns
    SMS_TYPES = {
        0: "all",
        1: "inbox",
        2: "sent",
        3: "draft",
        4: "outbox",
        5: "failed",
        6: "queued",
    }

    SMS_TYPES_REVERSED = {v: k for k, v in SMS_TYPES.items()}

    def __init__(
        self,
        s3_client,
        s3_bucket,
        raw_file_key,
        user_id,
        device_id,
        ts_updated,
        write_txt,
    ):
        super(SmsCollator, self).__init__(
            s3_client,
            s3_bucket,
            raw_file_key,
            user_id,
            device_id,
            ts_updated,
            "sms_log",
            write_txt,
        )

    # This function creates collated version of all relevant raw fields, and calculates
    # the unique hash
    @staticmethod
    def collate_entry(collated_entry, raw_entry):
        # Log if the raw entry has any unexpected fields
        if unexpected_fields := ",".join(
            [k for k in raw_entry.keys() if k not in SmsCollator.SCHEMA_RAW]
        ):
            LOGGER.warning(
                "Unexpected field(s) %s found for user: %s on device: %s",
                unexpected_fields,
                collated_entry.get("user_id"),
                collated_entry.get("device_id"),
            )

        # Some types of messages don't have a body, so, even though the body_hash is
        # part of the id, it may be blank
        collated_entry["body_hash"] = ""
        if "message_body" in raw_entry:
            collated_entry["message_body"] = raw_entry["message_body"].encode("utf-8")
            collated_entry["body_hash"] = hashlib.md5(
                collated_entry["message_body"]
            ).hexdigest()
        collated_entry["sms_type"] = SmsCollator._resolve_sms_type(
            BaseCollator._read_raw_field(["sms_type", "type"], raw_entry)
        )
        collated_entry["thread_id"] = BaseCollator._read_raw_field(
            "thread_id", raw_entry
        )
        collated_entry["contact_id"] = BaseCollator._read_raw_field(
            "contact_id", raw_entry
        )
        collated_entry["item_id"] = raw_entry["item_id"]
        # Some types of messages don't have an address, so, even though the sms_address
        # is part of the id, it may be blank
        collated_entry["sms_address"] = BaseCollator._read_raw_field(
            "sms_address", raw_entry
        )
        collated_entry["normalized_sms_address"] = collated_entry["sms_address"]
        if collated_entry["sms_address"] is not None:
            collated_entry["normalized_sms_address"] = re.sub(
                r"[\W_]", "", "".join(collated_entry["sms_address"].lower().split())
            )
        collated_entry["datetime"] = BaseCollator.parse_datetime(raw_entry["datetime"])
        collated_entry["id"] = hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    str(collated_entry["user_id"]),
                    collated_entry["device_id"],
                    str(collated_entry["datetime"]),
                    str(collated_entry["item_id"]),
                    collated_entry["sms_address"],
                    collated_entry["body_hash"],
                ]
            ).encode("utf-8")
        ).hexdigest()
        collated_entry["row_hash"] = SmsCollator.compute_row_hash(collated_entry)
        return True

    @staticmethod
    def compute_row_hash(entry):
        return hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    entry["id"],
                    entry["sms_type"],
                    entry["thread_id"],
                    entry["contact_id"],
                    entry["normalized_sms_address"],
                    entry["is_deleted"],
                ]
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _resolve_sms_type(sms_type):
        if sms_type is not None and int(sms_type) in SmsCollator.SMS_TYPES:
            return SmsCollator.SMS_TYPES[int(sms_type)]
        elif sms_type is not None:
            return "unknown"
        else:
            return None

    @staticmethod
    def _get_sms_type_id(sms_type):
        return SmsCollator.SMS_TYPES_REVERSED.get(sms_type)

    @staticmethod
    def create_txt_logs(all_existing_logs, device_id):
        # txt file contains logs from all the devices
        # meaning that the txt logs for a single device will actually contain logs for
        # multiple devices'''

        txt_logs = {}

        for log in all_existing_logs:
            try:
                log_data = {key: log[key] for key in SmsCollator.REQUIRED_FIELDS_TXT}

                # decoding the message body is necessary for the txt file to be valid
                log_data["message_body"] = log["message_body"].decode("utf-8")

            except KeyError:
                # sometimes message body is not present in the log, so we skip it
                log_data = {
                    key: log[key]
                    for key in SmsCollator.REQUIRED_FIELDS_TXT
                    if key != "message_body"
                }

            except AttributeError:
                # AttributeError is raised when the message body is None in existing
                # logs drop the message body from the log

                del log_data["message_body"]

            # setting datetime to epoch timestamp in milliseconds
            dt = int(log["datetime"].timestamp() * 1000)
            log_data["datetime"] = dt

            # converting sms type back to int as Rails expects it to be an int
            log_data["sms_type"] = SmsCollator._get_sms_type_id(log["sms_type"])

            outer_key = "{}:{}".format(dt, log["item_id"])
            txt_logs[outer_key] = log_data

        return txt_logs

    @staticmethod
    def create_txt_file(logs):
        # sorting the logs by datetime and item_id to ensure that the logs are
        # written in descending order in txt file
        sorted_logs = sorted(logs.items(), key=lambda x: x[0], reverse=True)

        return json.dumps([log[1] for log in sorted_logs])
