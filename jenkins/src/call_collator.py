"""Call log-specific logic for collation.  Contains knowledge of how to compute
the unique and row hashes for call log entries, as well as some information about the
call schema
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


class CallCollator(BaseCollator):
    SCHEMA_RAW = [
        "cached_name",  # str
        "call_type",  # str
        "item_id",  # int
        "phone_number",  # str
        "normalized_phone_number",  # str
        "datetime",  # timestamp
        "duration",  # int
    ]

    SCHEMA = SCHEMA_RAW + BaseCollator.BASE_SCHEMA

    REQUIRED_FIELDS_TXT = [
        "cached_name",
        "call_type",
        "item_id",
        "phone_number",
        "datetime",
        "duration",
    ]

    CALL_TYPES = {
        1: "incoming",
        2: "outgoing",
        3: "missed",
        4: "voicemail",
        5: "rejected",
        6: "blocked",
        7: "answered_externally",
    }

    CALL_TYPES_REVERSED = {v: k for k, v in CALL_TYPES.items()}

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
        super(CallCollator, self).__init__(
            s3_client,
            s3_bucket,
            raw_file_key,
            user_id,
            device_id,
            ts_updated,
            "call_log",
            write_txt,
        )

    # This function creates collated version of all relevant raw fields, and calculates
    # the unique hash
    @staticmethod
    def collate_entry(collated_entry, raw_entry):
        # Log if the raw entry has any unexpected fields
        if unexpected_fields := ",".join(
            [k for k in raw_entry.keys() if k not in CallCollator.SCHEMA_RAW]
        ):
            LOGGER.warning(
                "Unexpected field(s) %s found for user: %s on device: %s",
                unexpected_fields,
                collated_entry.get("user_id"),
                collated_entry.get("device_id"),
            )

        if not raw_entry["phone_number"]:
            return False
        collated_entry["cached_name"] = BaseCollator._read_raw_field(
            "cached_name", raw_entry
        )
        collated_entry["call_type"] = CallCollator._resolve_call_type(
            BaseCollator._read_raw_field("call_type", raw_entry)
        )
        collated_entry["item_id"] = raw_entry["item_id"]
        collated_entry["phone_number"] = raw_entry["phone_number"]
        collated_entry["normalized_phone_number"] = re.sub(
            r"[\W_]", "", "".join(raw_entry["phone_number"].lower().split())
        )
        collated_entry["datetime"] = BaseCollator.parse_datetime(
            int(raw_entry["datetime"])
        )
        collated_entry["duration"] = BaseCollator._read_raw_field("duration", raw_entry)
        if collated_entry["duration"] is not None:
            collated_entry["duration"] = int(collated_entry["duration"])
        collated_entry["id"] = hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    str(collated_entry["user_id"]),
                    collated_entry["device_id"],
                    str(collated_entry["datetime"]),
                    str(collated_entry["item_id"]),
                    collated_entry["phone_number"],
                ]
            ).encode("utf-8")
        ).hexdigest()
        collated_entry["row_hash"] = CallCollator.compute_row_hash(collated_entry)
        return True

    @staticmethod
    def compute_row_hash(entry):
        return hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    entry["id"],
                    entry["cached_name"],
                    entry["call_type"],
                    entry["normalized_phone_number"],
                    entry["duration"],
                    entry["is_deleted"],
                ]
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _resolve_call_type(call_type):
        if call_type is not None and int(call_type) in CallCollator.CALL_TYPES:
            return CallCollator.CALL_TYPES[int(call_type)]
        elif call_type is not None:
            return "unknown"
        else:
            return None

    @staticmethod
    def _get_call_type_id(call_type):
        return CallCollator.CALL_TYPES_REVERSED.get(call_type)

    @staticmethod
    def create_txt_logs(all_existing_logs, device_id):
        # txt file contains logs from all the devices
        # meaning that the txt logs for a single device will actually contain logs for
        # multiple devices'''

        txt_logs = {}

        for log in all_existing_logs:
            log_data = {
                key: log[key] for key in CallCollator.REQUIRED_FIELDS_TXT if key in log
            }

            # setting datetime to epoch timestamp in milliseconds
            dt = int(log["datetime"].timestamp() * 1000)
            log_data["datetime"] = dt

            # converting sms type back to int as Rails expects it to be an int
            log_data["call_type"] = CallCollator._get_call_type_id(log["call_type"])

            outer_key = "{}:{}".format(dt, log["item_id"])
            txt_logs[outer_key] = log_data

        return txt_logs

    @staticmethod
    def create_txt_file(logs):
        # sorting the logs by datetime and item_id to ensure that the logs are
        # written in descending order in txt file
        sorted_logs = sorted(logs.items(), key=lambda x: x[0], reverse=True)

        return json.dumps([log[1] for log in sorted_logs])
