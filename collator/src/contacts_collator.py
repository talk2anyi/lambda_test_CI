"""Contact log-specific logic for collation.  Contains knowledge of how to compute
the unique and row hashes for contact log entries, as well as some information about the
contact schema
"""

import ast
import hashlib
import json
import logging

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


class ContactsCollator(BaseCollator):
    SCHEMA_RAW = [
        "display_name",  # str
        "item_id",  # int
        "last_time_contacted",  # timestamp
        "photo_id",  # str
        "times_contacted",  # int
        "phone_numbers",  # str
    ]

    SCHEMA = SCHEMA_RAW + BaseCollator.BASE_SCHEMA

    REQUIRED_FIELDS_TXT = ["display_name", "item_id", "phone_numbers"]

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
        super(ContactsCollator, self).__init__(
            s3_client,
            s3_bucket,
            raw_file_key,
            user_id,
            device_id,
            ts_updated,
            "contact_list",
            write_txt,
        )

    # This function creates collated version of all relevant raw fields, and calculates
    # the unique hash
    @staticmethod
    def collate_entry(collated_entry, raw_entry):
        # Log if the raw entry has any unexpected fields
        if unexpected_fields := ",".join(
            [k for k in raw_entry.keys() if k not in ContactsCollator.SCHEMA_RAW]
        ):
            LOGGER.warning(
                "Unexpected field(s) %s found for user: %s on device: %s",
                unexpected_fields,
                collated_entry.get("user_id"),
                collated_entry.get("device_id"),
            )

        collated_entry["display_name"] = BaseCollator._read_raw_field(
            "display_name", raw_entry
        )
        collated_entry["last_time_contacted"] = BaseCollator._read_raw_field(
            "last_time_contacted", raw_entry
        )
        if collated_entry["last_time_contacted"] is not None:
            collated_entry["last_time_contacted"] = BaseCollator.parse_datetime(
                int(collated_entry["last_time_contacted"])
            )
        collated_entry["photo_id"] = BaseCollator._read_raw_field("photo_id", raw_entry)
        collated_entry["times_contacted"] = BaseCollator._read_raw_field(
            "times_contacted", raw_entry
        )
        collated_entry["item_id"] = raw_entry["item_id"]
        collated_entry["phone_numbers"] = BaseCollator._read_raw_field(
            "phone_numbers", raw_entry
        )
        if collated_entry["phone_numbers"] is not None:
            collated_entry["phone_numbers"] = json.dumps(
                collated_entry["phone_numbers"]
            )
        collated_entry["id"] = hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    str(collated_entry["user_id"]),
                    collated_entry["device_id"],
                    str(collated_entry["item_id"]),
                ]
            ).encode("utf-8")
        ).hexdigest()
        collated_entry["row_hash"] = ContactsCollator.compute_row_hash(collated_entry)
        return True

    @staticmethod
    def compute_row_hash(entry):
        return hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    entry["id"],
                    entry["display_name"],
                    entry["last_time_contacted"],
                    entry["times_contacted"],
                    entry["photo_id"],
                    entry["phone_numbers"],
                    entry["is_deleted"],
                ]
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def create_txt_logs(all_existing_logs, device_id):
        # txt file for app packages contains logs only for a particular device id and
        # only the latest apps

        deleted_ids = set([log["id"] for log in all_existing_logs if log["is_deleted"]])

        logs = [
            {key: log[key] for key in ContactsCollator.REQUIRED_FIELDS_TXT}
            for log in all_existing_logs
            if log["id"] not in deleted_ids and log["device_id"] == device_id
        ]

        txt_logs = {}

        for log in logs:
            if "item_id" not in log:
                raise Exception(
                    "Log collation failed: Missing item_id key in contact_list"
                )
            phone_numbers = log.get("phone_numbers")
            if phone_numbers:
                log["phone_numbers"] = ContactsCollator.dedupe_phone_numbers(
                    phone_numbers
                )
            txt_logs[log["item_id"]] = log

        return txt_logs

    @staticmethod
    def create_txt_file(logs):
        # sorting the logs by datetime and item_id to ensure that the logs are
        # written in ascending order in txt file
        sorted_logs = sorted(logs.items(), key=lambda x: x[0], reverse=False)

        return json.dumps([log[1] for log in sorted_logs])

    @staticmethod
    def dedupe_phone_numbers(phone_numbers):
        """Remove duplicate phone numbers based on the phone_number"""
        if not phone_numbers:
            return []
        deduped_phone_numbers = {}
        phone_numbers = ast.literal_eval(phone_numbers)
        for phone_number in phone_numbers:
            key = phone_number.get("normalized_phone_number") or phone_number.get(
                "phone_number"
            )
            if not key:
                # Ignore "phone numbers" that have no phone number
                continue
            existing = deduped_phone_numbers.get(key)
            if not existing or existing.get("item_id", 0) < phone_number["item_id"]:
                deduped_phone_numbers[key] = phone_number
        return list(deduped_phone_numbers.values())
