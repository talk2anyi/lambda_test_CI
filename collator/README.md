# Collator notes

Collator is an AWS Lambda function that processes uploaded raw phone logs into more usable versions for Credit Service, Feature Service, and rails. Also see [documentation on Slab](https://branch.slab.com/posts/orchestrator-and-log-collator-11j77ia8).

## Steps in the pipeline

1. The functions are defined in Lambda
   1. Primary collator: [s3_log_collation_v2](https://ap-south-1.console.aws.amazon.com/lambda/home?region=ap-south-1#/functions/s3_log_collation_v2?tab=configure)
   1. DLQ Collator: [s3_log_collation_dlq_v2](https://ap-south-1.console.aws.amazon.com/lambda/home?region=ap-south-1#/functions/s3_log_collation_dlq_v2?tab=configure)
      1. DLQ Collator is used when the primary Collator fails to process logs. It has more memory and a longer timeout.
1. S3 triggers the Lambda function when any file is uploaded to `uploads/users/`. The ultimate destination of a raw logs file will be `uploads/users/{user_id}/{device_serial_number}/{device_id}/`
   1. In production, we do not have `device_serial_number` any more, so this folder is usually called `unknown`
   1. The trigger is defined in S3 in each bucket's properties tab in the Event notifications section:
      1. [branch-in-production](https://s3.console.aws.amazon.com/s3/buckets/branch-in-production?region=ap-south-1&tab=properties)
      1. [branch-in-staging](https://s3.console.aws.amazon.com/s3/buckets/branch-in-staging?region=ap-south-1&tab=properties)
      1. [branch-development](https://s3.console.aws.amazon.com/s3/buckets/branch-development?region=us-west-2&tab=properties)
1. Collator gets each uploaded raw log file from s3, then saves a collated version to `collated_logs/`.
   1. Daily diffs are saved to `collated_logs/diff/`
   1. Fully updated parquet files are saved to `collated_logs/current/`
   1. A `txt`-format version for each log type for each device is saved to `collated_logs/user-{user_id}/device-{device_id}/`

## Docker notes

* Use `make compose` to build the lambda container along with s3 container
* Use `make up` to run the container before running either of the test suites
* Run `make test` to run the unit test suite
* Run `make integration` to run the integration test suite

## Build and use the Collator Docker image in AWS Lambda

A Docker image for Collator is saved in [ECR](https://ap-south-1.console.aws.amazon.com/ecr/repositories/private/987051346539/ml-images?region=ap-south-1) in `ap-south-1` in the `ml-images` directory.

### Build and push the image

Notes:

- The commands below includes a timestamp to differentiate between different image versions. Currently this is: `2023-09-06`
- As of September 2023, the [amazon/aws-lambda-python:3.10](https://hub.docker.com/r/amazon/aws-lambda-python/tags) image only supports the AMD architecture.

```sh
# Set the AWS region
AWS_REGION='ap-south-1'

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin 987051346539.dkr.ecr.$AWS_REGION.amazonaws.com

# Set the version timestamp
IMAGE_VERSION='2023-09-11'

# Build the AMD image locally
docker build --platform linux/amd64 --tag 987051346539.dkr.ecr.$AWS_REGION.amazonaws.com/ml-images:lambda_collator-$IMAGE_VERSION-amd64 .

# Push the AMD image to ECR
docker push 987051346539.dkr.ecr.$AWS_REGION.amazonaws.com/ml-images:lambda_collator-$IMAGE_VERSION-amd64
```

### Use the image

Important: deploying a new image automatically deploys the code as the `$LATEST` version, which some environments use. See the `Aliases` tab to get an idea of what to expect.

1. Deploy the image
   1. On the `Image` tab of each function (linked above)
      1. Click `Deploy new image`
      1. Click `Browse images`
      1. Select the appropriate image by tag and digest
      1. Click `Save`
      1. Wait for AWS Lambda to update the function
1. Use the Staging environment to test that everything is working as expected
1. When you're ready to use this image in Production
   1. On the `Versions` tab of each function (linked above)
      1. Click `Publish new version` to create a new version of the lambda function
      1. Write a short description of what has changed
      1. Click `Publish`
   1. On the `Aliases` tab, edit the Production alias to use the new version
1. Ensure that the new version is working as expected in Production
