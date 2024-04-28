import io
import logging
import os
from datetime import datetime
from typing import Any

import boto3
import pandas as pd
import chess

import utils
from models import S3Record

# Configure logging
logger = logging.getLogger("data-preprocessing")
logger.setLevel(logging.INFO)

# The AWS region
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

# The input bucket name
PREPROCESSED_INPUT_BUCKET_NAME = os.environ.get("PREPROCESSED_INPUT_BUCKET_NAME")

# The output bucket name
PREPROCESSED_OUTPUT_BUCKET_NAME = os.environ.get("PREPROCESSED_OUTPUT_BUCKET_NAME")

# Configure S3 client
s3_client = boto3.client("s3", region_name=AWS_REGION)

# Configure chess engine
ENGINE_PATH = "/usr/local/bin/stockfish"

chess_engine = utils.get_engine_stockfish(ENGINE_PATH)

def lambda_handler(event, context):
    """
    Perform data preprocessing on new data received

    :param event: AWS S3 Event received
    :param context:
    :return:
    """
    s3_record = S3Record(event)

    logger.info(
        "Received event: %s on bucket: %s for object: %s",
        s3_record.event_name,
        s3_record.bucket_name,
        s3_record.object
    )

    if pre_checks_before_processing(s3_record.object, find_tag="ProcessedTime"):
        return

    # Load the data recently uploaded to the bucket
    logger.info("Loading the data recently uploaded to the bucket")
    data = retrieve_s3_object(key=s3_record.object)

    logger.info("Using stockfish to calculate.")
    data = run_stockfish(data)

    logger.info("Uploading data to S3.")
    upload_to_output_bucket(data, key=s3_record.object)

    tag_as_processed(key=s3_record.object)
    logger.info(
        "Data preprocessing complete and uploaded to %s bucket.",
        PREPROCESSED_OUTPUT_BUCKET_NAME,
    )
    return event


def pre_checks_before_processing(key: str, find_tag: str, client: Any = s3_client) -> bool:
    """
    Check that the object is a csv file and has not been processed previously.

    :param client: boto3 client configured to use s3
    :param key: The full path for to object
    :param find_tag: Tag to find on the object
    :return: bool
    """
    object_tags = client.get_object_tagging(
        Bucket=PREPROCESSED_INPUT_BUCKET_NAME, Key=key
    )
    if ".parquet" not in key:
        logger.info("Will not process, expected object to be a parquet.")
        return True
    else:
        for tag in object_tags["TagSet"]:
            if find_tag in tag:
                logger.info("Object has previously been processed.")
                return True
    return False


def retrieve_s3_object(key: str, client: Any = s3_client) -> pd.DataFrame:
    """
    Get the csv file from the bucket and return as a DataFrame.

    :param client: boto3 client configured to use s3
    :param key: The full path for to object
    :return: DataFrame
    """
    s3_object = client.get_object(Bucket=PREPROCESSED_INPUT_BUCKET_NAME, Key=key)
    return pd.read_parquet(io.BytesIO(s3_object["Body"].read()))


def upload_to_output_bucket(data: pd.DataFrame, key: str, client: Any = s3_client) -> None:
    """
    Upload the file object to the output s3 bucket.

    :param client: boto3 client configured to use s3
    :param file_obj: The DataFrame as a csv
    :param key: The full path to the object destination
    :return:
    """
    file_obj = io.BytesIO()
    data.to_parquet(file_obj, index=False)
    file_obj.seek(0)
    client.put_object(
        Body=file_obj,
        Bucket=PREPROCESSED_OUTPUT_BUCKET_NAME,
        Tagging="ProcessedTime=%s" % str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        Key=key
    )


def tag_as_processed(key: str, client: Any = s3_client) -> None:
    """
    Add a tag to the csv that has now been processed.

    :param key: Key of the object to get.
    :param client: boto3 client configured to use s3
    :return:
    """
    client.put_object_tagging(
        Bucket=PREPROCESSED_INPUT_BUCKET_NAME,
        Tagging={
            "TagSet": [
                {
                    "Key": "ProcessedTime",
                    "Value": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                },
            ]
        },
        Key=key
    )


def run_stockfish(data: pd.DataFrame, engine = chess_engine) -> pd.DataFrame:
    data = utils.load_json(data, ['fen'])
    metrics = data['fen'].apply(lambda fens: utils.game_metrics(fens, engine))
    data[['eval', 'eval_replaced', 'winning_chance', 'losing_chance']] = pd.DataFrame(metrics.tolist(), index=data.index)
    return data
