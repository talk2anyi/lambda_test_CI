"""Base class containing the shared update and deletion logic used
by all log types during collation"""

import datetime
import gzip
import io
import json
import logging
from abc import ABC, abstractmethod

from botocore.exceptions import ClientError
from ddtrace import patch, tracer
from parquet import reader, writer

patch(logging=True)

FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d]"
    " [dd.service=%(dd.service)s dd.env=%(dd.env)s dd.version=%(dd.version)s"
    " dd.trace_id=%(dd.trace_id)s dd.span_id=%(dd.span_id)s] - %(message)s"
)

logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger("collator")
LOGGER.setLevel(logging.INFO)


class BaseCollator(ABC):
    BASE_SCHEMA = [
        "device_id",  # str
        "row_hash",  # str
        "id",  # str
        "is_deleted",  # bool
        "user_id",  # int
        "ts_updated",  # timestamp
    ]

    CURRENT_COLLATED_LOGS_KEY = "collated_logs/current/{}/user={}/logs.parquet"
    CHANGED_LOGS_KEY = "collated_logs/diff/{}/ts_update={}/user={}/logs.parquet"
    TXT_LOGS_KEY = "collated_logs/user-{}/device-{}/collated_{}.txt"
    MISSING_KEY_ERROR = "NoSuchKey"

    def __init__(
        self,
        s3_client,
        s3_bucket,
        raw_file_key,
        user_id,
        device_id,
        ts_updated,
        log_type,
        write_txt=None,
    ):
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.user_id = int(user_id)
        self.device_id = device_id
        self.raw_file_key = raw_file_key
        self.log_type = log_type
        self.ts_updated = ts_updated
        self.write_txt = write_txt if write_txt is not None else True
        self.ids = set()
        self.key = self.CURRENT_COLLATED_LOGS_KEY.format(self.log_type, self.user_id)
        self.diff_key = self.CHANGED_LOGS_KEY.format(
            self.log_type, self._batch_ts(self.ts_updated), self.user_id
        )
        self.txt_logs_key = self.TXT_LOGS_KEY.format(
            self.user_id, self.device_id, self.log_type
        )
        self.existing_logs = None
        self.existing_row_hashes = None
        self.new_logs = []
        self.all_existing_logs_count = 0
        self.existing_logs_count = 0
        self.new_logs_count = 0
        self.deleted_logs_count = 0
        self.total_logs_count = 0

    def collate(self):
        """Primary public method that does all parts of collation"""
        self._retrieve_existing_entries()
        self._process_new_logs()
        self._process_deletions()
        self._write_updates()

    @tracer.wrap("_retrieve_existing_entries")
    def _retrieve_existing_entries(self):
        """Initializes the collator with the existing collated entries stored on S3"""
        try:
            result = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.key)
            self.all_existing_logs = reader(result["Body"])
            self.existing_logs = self._create_unique_set(self.all_existing_logs)
            self.existing_row_hashes = [log["row_hash"] for log in self.existing_logs]
            self.all_existing_logs_count = len(self.all_existing_logs)
            self.existing_logs_count = len(self.existing_logs)
        except ClientError as ex:
            # If this is the first log seen for a given user, simply create an empty df
            if ex.response["Error"]["Code"] == self.MISSING_KEY_ERROR:
                self.all_existing_logs = []
                self.existing_logs = []
                self.existing_row_hashes = set()
            else:
                raise ex

    @tracer.wrap("_process_new_logs")
    def _process_new_logs(self):
        """Creates new collated log entries for all new or updated raw entries"""
        body = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.raw_file_key)[
            "Body"
        ].read()

        # Attempt to decompress logs
        try:
            body = gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb").read()
        except gzip.BadGzipFile:
            # Assume this is a text file, not a gzip file
            pass

        try:
            raw_entries = json.loads(body)
        except json.decoder.JSONDecodeError:
            LOGGER.error(
                "Unable to decode JSON in file: %s for user: %s on device: %s",
                self.raw_file_key,
                self.user_id,
                self.device_id,
            )
            raise

        new_logs = []
        for raw_entry in raw_entries:
            collated_entry = {}
            collated_entry["user_id"] = self.user_id
            collated_entry["device_id"] = self.device_id
            collated_entry["is_deleted"] = False
            if not self.collate_entry(collated_entry, raw_entry):
                continue
            self.ids.add(collated_entry["id"])
            if collated_entry["row_hash"] not in self.existing_row_hashes:
                collated_entry["ts_updated"] = self.ts_updated
                new_logs.append(collated_entry)
        self.new_logs.extend(new_logs)
        self.new_logs_count = len(new_logs)

    @tracer.wrap("_process_deletions")
    def _process_deletions(self):
        """Creates new collated entries for each deleted existing entry"""
        deleted_logs = []
        # Since only new SMS logs are uploaded by devices, we are unable to identify
        # deletions in that case
        if self.log_type == "sms_log":
            return
        for log in self.existing_logs:
            if (
                not log["is_deleted"]
                and log["device_id"] == self.device_id
                and log["id"] not in self.ids
            ):
                deleted_entry = log.copy()
                deleted_entry["is_deleted"] = True
                deleted_entry["row_hash"] = self.compute_row_hash(deleted_entry)
                deleted_entry["ts_updated"] = self.ts_updated
                deleted_logs.append(deleted_entry)
        self.new_logs.extend(deleted_logs)
        self.deleted_logs_count = len(deleted_logs)

    @tracer.wrap("_write_updates")
    def _write_updates(self):
        """Writes updated collated log parquet and txt files back to S3"""

        # combining existing logs and new logs
        with tracer.trace("_write_updates.combine"):
            self.all_existing_logs.extend(self.new_logs)
        self.total_logs_count = len(self.all_existing_logs)

        # Handling future timestamp issue in SMS logs
        if self.log_type == "sms_log":
            with tracer.trace("_write_updates.future_timestamp_handler"):
                self.all_existing_logs = BaseCollator.future_timestamp_handler(
                    self.all_existing_logs
                )

        # writing the combined logs to parquet file in s3
        with tracer.trace("_write_updates.write_parquet_combined"):
            self._write_logs(self.all_existing_logs, self.key, "parquet")

        # creating txt logs from the combined logs:
        # 1. selecting the required keys
        # 2. removing the deleted logs except for sms_logs
        if self.write_txt:
            with tracer.trace("_write_updates.write_txt"):
                txt_logs = self.create_txt_logs(self.all_existing_logs, self.device_id)
                self._write_logs(txt_logs, self.txt_logs_key, "txt")

        # Then, write the new changes to be processed by the batch job, and merge with
        # any existing changes
        with tracer.trace("_write_updates.write_parquet_diff"):
            try:
                result = self.s3_client.get_object(
                    Bucket=self.s3_bucket, Key=self.diff_key
                )
                diff_logs = reader(result["Body"])
                diff_logs.extend(self.new_logs)
            except ClientError as ex:
                # If this is the first change seen for a given user, simply write current
                # changes
                if ex.response["Error"]["Code"] == self.MISSING_KEY_ERROR:
                    diff_logs = self.new_logs
                else:
                    raise ex
            self._write_logs(diff_logs, self.diff_key, file_format="parquet")

    def _create_unique_set(self, logs):
        # We should only consider the most recent version of a log as having as
        # valid row_hash to match against
        hashes = {}
        for log in logs:
            id = log["id"]
            if not (id in hashes and hashes[id]["ts_updated"] > log["ts_updated"]):
                hashes[id] = log
        return hashes.values()

    def _write_logs(self, logs, key, file_format):
        if len(logs) > 0:
            if file_format == "parquet":
                out = writer(logs)
                body = out.to_pybytes()
            elif file_format == "txt":
                body = self.create_txt_file(logs)

            self.s3_client.put_object(Bucket=self.s3_bucket, Key=key, Body=body)

    def _batch_ts(self, dt):
        # Change granularity of diff period for backfill, which only applies to past
        # dates
        if dt < datetime.datetime(2016, 1, 1):
            return datetime.datetime(
                dt.year, self._month_to_quarter(dt.month), 1
            ).strftime("%Y-%m-%d")
        if dt < datetime.datetime(2017, 7, 1):
            return datetime.datetime(dt.year, dt.month, 1).strftime("%Y-%m-%d")
        return datetime.datetime(dt.year, dt.month, dt.day).strftime("%Y-%m-%d")

    def _month_to_quarter(self, month):
        if month < 4:
            return 1
        if month < 7:
            return 4
        if month < 10:
            return 7
        return 10

    @staticmethod
    def _read_raw_field(fields, raw_entry):
        if not isinstance(fields, list):
            fields = [fields]
        for field in fields:
            try:
                return raw_entry[field]
            except KeyError:
                continue
        return None

    @staticmethod
    def parse_datetime(raw_dt):
        # datetime can be in milliseconds or seconds
        if len(str(raw_dt)) > 10:
            dt = raw_dt / 1000
        else:
            dt = raw_dt
        try:
            result_dt = datetime.datetime.fromtimestamp(dt)

        except ValueError:
            result_dt = None
        return result_dt

    @staticmethod
    def combine_hash_fields(fields):
        cleaned_fields = []
        for field in fields:
            if field is not None:
                cleaned_fields.append(str(field))
        return ":".join(cleaned_fields)

    @staticmethod
    def future_timestamp_handler(logs):
        # Rarely, SMS logs have timestamps in the future, which can cause problems.
        # Impute future timestamps with ts_updated.
        corrected_logs = []
        for log in logs:
            if log["datetime"] > log["ts_updated"]:
                log["datetime"] = log["ts_updated"]

            corrected_logs.append(log)

        return corrected_logs

    @abstractmethod
    def collate_entry(collated_entry, raw_entry):
        pass

    @abstractmethod
    def compute_row_hash(entry):
        pass

    @abstractmethod
    def create_txt_logs(all_existing_logs, device_id=None):
        pass

    @abstractmethod
    def create_txt_file(logs):
        pass
