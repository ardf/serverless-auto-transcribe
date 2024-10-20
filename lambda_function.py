"""
This lambda function is triggered whenever an object is added to S3 bucket 
named demo-auto-transcribe.
It triggers the AWS Transcribe API and the resulted transcription is 
stored in the same bucket with `transcription/` prefix.
"""

import re
import json
import uuid
import logging

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

transcribe_client = boto3.client("transcribe")

SUPPORTED_FORMATS = {
    "mp3",
    "mp4",
    "wav",
    "flac",
    "mov",
    "mpg",
    "mpeg",
    "m4a", 
}

def sanitize_job_name(name):
    """
    Sanitize the job name to conform to AWS Transcribe's expected format.
    """
    # Replace any invalid characters with an underscore
    return re.sub(r'[^0-9a-zA-Z._-]', '_', name)

def lambda_handler(event, context):
    """
    The handler function to be invoked when the event notification is triggered
    """
    records = event.get("Records",[])
    for record in records:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        file_extension = object_key.split(".")[-1].lower()

        # Check if the file type is supported
        if file_extension in SUPPORTED_FORMATS:
            logger.info("File %s is a supported format: %s", object_key, file_extension)

            base_job_name = object_key.split(".")[0]
            transcription_job_name = sanitize_job_name(f"{base_job_name}_{str(uuid.uuid4())}")

            media_file_url = f"s3://{bucket_name}/{object_key}"

            output_bucket_name = bucket_name

            try:
                transcribe_client.start_transcription_job(
                    TranscriptionJobName=transcription_job_name,
                    Media={"MediaFileUri": media_file_url},
                    MediaFormat=file_extension,
                    LanguageCode="en-US",
                    OutputBucketName=output_bucket_name,
                    OutputKey=f"transcriptions/{transcription_job_name}.json",
                )
                logger.info(
                    "Transcription job %s started successfully.",
                    transcription_job_name,
                )
            except Exception as e:
                logger.error("Error starting transcription job: %s", str(e))
                raise e
        else:
            logger.info(
                "File %s is not a supported format. Skipping transcription.", object_key
            )

    return {
        "statusCode": 200,
        "body": json.dumps("Transcription function executed successfully."),
    }
