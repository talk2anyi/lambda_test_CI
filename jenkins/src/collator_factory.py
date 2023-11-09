"""Builds collators associated with each log type"""

from app_collator import AppCollator
from call_collator import CallCollator
from contacts_collator import ContactsCollator
from sms_collator import SmsCollator


class CollatorFactory:
    @staticmethod
    def build(
        log_type,
        s3_client,
        s3_bucket,
        record,
        user_id,
        device_id,
        ts_updated,
        write_txt=None,
    ):
        # Given log_type, return the appropriate initialized collator
        if log_type == "app_packages":
            return AppCollator(
                s3_client,
                s3_bucket,
                record["s3"]["object"]["key"],
                user_id,
                device_id,
                ts_updated,
                write_txt,
            )
        elif log_type == "contact_list":
            return ContactsCollator(
                s3_client,
                s3_bucket,
                record["s3"]["object"]["key"],
                user_id,
                device_id,
                ts_updated,
                write_txt,
            )
        elif log_type == "sms_log":
            return SmsCollator(
                s3_client,
                s3_bucket,
                record["s3"]["object"]["key"],
                user_id,
                device_id,
                ts_updated,
                write_txt,
            )
        elif log_type == "call_log":
            return CallCollator(
                s3_client,
                s3_bucket,
                record["s3"]["object"]["key"],
                user_id,
                device_id,
                ts_updated,
                write_txt,
            )
        else:
            raise ValueError(f"Unsupported log type: '{log_type}'")
