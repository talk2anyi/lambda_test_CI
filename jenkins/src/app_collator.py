"""App package-specific logic for collation.  Contains knowledge of how to compute
the unique and row hashes for app package log entries, as well as some information about
the app schema
"""

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


class AppCollator(BaseCollator):
    SCHEMA_RAW = [
        "package_name",  # str
    ]

    SCHEMA = SCHEMA_RAW + BaseCollator.BASE_SCHEMA

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
        super(AppCollator, self).__init__(
            s3_client,
            s3_bucket,
            raw_file_key,
            user_id,
            device_id,
            ts_updated,
            "app_packages",
            write_txt,
        )

    # This function creates collated version of all relevant raw fields, and calculates
    # the unique hash
    @staticmethod
    def collate_entry(collated_entry, raw_entry):
        # Log if the raw entry has any unexpected fields
        if unexpected_fields := ",".join(
            [k for k in raw_entry.keys() if k not in AppCollator.SCHEMA_RAW]
        ):
            LOGGER.warning(
                "Unexpected field(s) %s found for user: %s on device: %s",
                unexpected_fields,
                collated_entry.get("user_id"),
                collated_entry.get("device_id"),
            )

        if not raw_entry["package_name"]:
            return False
        collated_entry["package_name"] = "".join(
            raw_entry["package_name"].lower().split()
        )
        collated_entry["id"] = hashlib.md5(
            BaseCollator.combine_hash_fields(
                [
                    str(collated_entry["user_id"]),
                    collated_entry["device_id"],
                    collated_entry["package_name"],
                ]
            ).encode("utf-8")
        ).hexdigest()
        collated_entry["row_hash"] = AppCollator.compute_row_hash(collated_entry)
        return True

    @staticmethod
    def compute_row_hash(entry):
        return hashlib.md5(
            BaseCollator.combine_hash_fields(
                [entry["id"], str(entry["is_deleted"])]
            ).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def create_txt_logs(all_existing_logs, device_id):
        """txt file for app packages contains logs only for a particular device id and
        only the latest logs"""

        deleted_ids = set([log["id"] for log in all_existing_logs if log["is_deleted"]])

        txt_logs = [
            {"package_name": log["package_name"]}
            for log in all_existing_logs
            if log["id"] not in deleted_ids and log["device_id"] == device_id
        ]

        return txt_logs

    @staticmethod
    def create_txt_file(logs):
        return json.dumps(logs)
