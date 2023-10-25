"""Lambda function for S3 log collation. This gets triggered when a file
is uploaded to /uploads/users/"""

import json
import logging
import os
import traceback
from datetime import datetime

import boto3
from collator_factory import CollatorFactory
from datadog_lambda.metric import lambda_metric
from ddtrace import patch, tracer

patch(logging=True)

FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d]"
    " [dd.service=%(dd.service)s dd.env=%(dd.env)s dd.version=%(dd.version)s"
    " dd.trace_id=%(dd.trace_id)s dd.span_id=%(dd.span_id)s] - %(message)s"
)

logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger("collator")
LOGGER.setLevel(logging.INFO)

BUCKET_PREFIX = "branch-in-"

VALID_LOG_TYPES = [
    "sms_log",
    "call_log",
    "contact_list",
    "app_packages",
]


# Environment variable controls whether to write collated logs as text files to S3
WRITE_TXT = os.getenv("WRITE_TXT", default="true").lower() == "true"


def lambda_handler(event, context):
    """The function that gets triggered by Lambda. Happens for both S3 uploads (normal
    case) and via the DLQ for the normal log collation (when memory is exceeded and
    needs to be run with more RAM)
    """

    for record in event["Records"]:
        try:
            if "Sns" in record:
                # The initial s3 records are in the message field
                message = json.loads(record["Sns"]["Message"])
                s3_records = message["Records"]
            else:
                s3_records = [record]

            start_time_total = datetime.utcnow()

            log_types = []
            for s3_record in s3_records:
                filename = s3_record["s3"]["object"]["key"]
                try:
                    _, _, user_id, device_serial, device_id, log_type, _ = (
                        filename.split("/")
                    )
                except ValueError:
                    # Doesn't match the file pattern
                    LOGGER.error(
                        "File uploaded outside of expected directory: %s", filename
                    )
                    continue

                span = tracer.current_span()
                if span:
                    span.set_tag("info.user_id", user_id)
                    span.set_tag("info.device_id", device_id)

                collate_logs_for_user(
                    s3_record, user_id, device_id, device_serial, log_type
                )
                log_types.append(log_type)

            seconds_total = (datetime.utcnow() - start_time_total).total_seconds()

            LOGGER.info(
                "Done collating all logs: %s for user: %s on device: %s in %s seconds",
                ",".join(log_types),
                user_id,
                device_id,
                seconds_total,
            )

        except:
            for line in traceback.format_exc().split("\n"):
                LOGGER.error(line)
            raise


@tracer.wrap("collate_logs_for_user")
def collate_logs_for_user(record, user_id, device_id, device_serial, log_type):
    """Main collation function"""

    LOGGER.info(
        "Collating %s logs for user: %s on device: %s", log_type, user_id, device_id
    )

    if log_type not in VALID_LOG_TYPES:
        LOGGER.error("Unexpected log type : %s for user: %s", log_type, user_id)
        return

    with tracer.trace("create_s3_client"):
        s3_bucket = record["s3"]["bucket"]["name"]
        session = boto3.session.Session()
        s3_client = session.client(
            "s3", region_name=record["awsRegion"], endpoint_url=os.getenv("S3_ENDPOINT")
        )

    ts_update = datetime.utcnow()

    # New version of collation
    with tracer.trace("build_collator"):
        collator = CollatorFactory.build(
            log_type,
            s3_client,
            s3_bucket,
            record,
            user_id,
            device_id,
            ts_update,
            WRITE_TXT,
        )

    start_time_log = datetime.utcnow()

    with tracer.trace("collate"):
        collator.collate()

    seconds_log = (datetime.utcnow() - start_time_log).total_seconds()

    LOGGER.info(
        "Finished collating %s logs for user: %s on device: %s in %s seconds. %s"
        " previous, %s previous unique, %s new, %d deleted, %d total",
        log_type,
        user_id,
        device_id,
        seconds_log,
        collator.all_existing_logs_count,
        collator.existing_logs_count,
        collator.new_logs_count,
        collator.deleted_logs_count,
        collator.total_logs_count,
    )

    span = tracer.current_span()
    if span:
        span.set_tag("info.user_id", user_id)
        span.set_tag("info.device_id", device_id)
        span.set_tag("info.log_type", log_type)
        span.set_tag("info.all_existing_logs_count", collator.all_existing_logs_count)
        span.set_tag("info.total_logs_count", collator.total_logs_count)

    lambda_metric(
        metric_name="collator.seconds_log",
        value=seconds_log,
        tags=[f"log_type:{log_type}"],
    )
