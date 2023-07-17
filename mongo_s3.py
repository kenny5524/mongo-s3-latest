import threading
import datetime
import logging
import os
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from urllib.parse import quote_plus
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
import time
import psutil
from io import BytesIO
import numpy as np


# Load the .env file
load_dotenv('.env')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')  # replace with your AWS access key ID
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')  # replace with your AWS secret access key
BUCKET_NAME = os.getenv('BUCKET_NAME')  # replace with your target S3 bucket name

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(f'mongodb_stream_{timestamp}.log', when='M', interval=20, backupCount=20)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


def get_last_resume_token(collection_name, resume_token_collection):
    doc = resume_token_collection.find_one({'_id': collection_name})
    return doc['resume_token'] if doc and 'resume_token' in doc else None


def store_last_resume_token(collection_name, resume_token, resume_token_collection):
    resume_token_collection.replace_one(
        {'_id': collection_name},
        {'_id': collection_name, 'resume_token': resume_token},
        upsert=True
    )


def process_change_event(change):
    operation_type = change.get("operationType")
    full_document = change.get("fullDocument")
    resume_token = change.get("_id")
    if full_document.get('_id') is not None:
        full_document['_id'] = str(full_document['_id'])
    result = {'operationType': operation_type}
    result.update(full_document)
    return result


def create_directory(bucket_name, directory_name):
    # Check if the directory already exists
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory_name)

    if 'Contents' not in response:
        # Directory doesn't exist, create it
        s3.put_object(Bucket=bucket_name, Key=f"{directory_name}/")
        log.info(f"Created directory: {directory_name}")



def write_parquet_file(data, collection_name):
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    directory_name = collection_name
    create_directory(BUCKET_NAME, directory_name)
    base_filename = f"{directory_name}/{collection_name}_{timestamp}"
    df = pd.DataFrame(data)

    if 'BallAction' in df.columns:
        df['BallAction'] = df['BallAction'].astype(str)

    # Handle None values in the DataFrame
    str_columns = df.select_dtypes(include=['object']).columns
    df[str_columns] = df[str_columns].fillna('')

    # Fill NaN values with 999999999 in numeric columns
    num_columns = df.select_dtypes(include=[np.number]).columns
    df[num_columns] = df[num_columns].fillna(9999999999)

    if 'BallAction' in df.columns:
        df['BallAction'] = df['BallAction'].astype(str)

    if 'PadNumber' in df.columns:
        df['PadNumber'] = df['PadNumber'].astype(str)

    #if 'TimeSubmitted_UTC' in df.columns:
       # df['TimeSubmitted_UTC'] = pd.to_datetime(df['TimeSubmitted_UTC'])


    try:
        # Write DataFrame to Parquet file in memory
        table = pa.Table.from_pandas(df)
        parquet_data = BytesIO()
        pq.write_table(table, parquet_data, version="2.4")

        # Upload the Parquet data as a single file with dynamic directory
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{base_filename}.parquet",
            Body=parquet_data.getvalue(),
            ContentType='application/octet-stream'
        )

        log.info(f"Successfully written to {base_filename}.parquet in S3 bucket {BUCKET_NAME}")
    except Exception as e:
        log.error(f"Error writing {base_filename} to S3: {str(e)}")


def watch_collection(collection, resume_token_collection, event):
    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}}}]
    last_resume_token = get_last_resume_token(collection.name, resume_token_collection)
    resume_options = {'resume_after': last_resume_token} if last_resume_token else {}

    changes = []
    last_write_time = time.time()

    try:
        with collection.watch(pipeline, **resume_options) as stream:
            event.wait()  # Wait for the event to be set before starting the change stream
            log.info(f"Change stream for collection '{collection.name}' started. Resume Token: {last_resume_token}")
            c=0
            for change in stream:
                op_type = change['operationType']
                current_resume_token = change['_id']
                document_id = change['fullDocument']['_id']
                log.info(f"Change detected: {collection.name}: {op_type}: {document_id}: {current_resume_token}")
                changes.append(process_change_event(change))
                last_resume_token = current_resume_token
                c += 1
                if time.time() - last_write_time >= 1200:
                    write_parquet_file(changes, collection.name)
                    changes.clear()
                    last_write_time = time.time()
                    store_last_resume_token(collection.name, last_resume_token, resume_token_collection)

                # Monitor CPU usage
                cpu_percent = psutil.cpu_percent(interval=None)  # Set interval to None for instantaneous value
                log.info(f"CPU Usage: {cpu_percent}%")

                # Monitor memory usage
                memory_info = psutil.virtual_memory()
                used_memory = memory_info.used
                total_memory = memory_info.total
                memory_percent = memory_info.percent
                log.info(f"Memory Usage: {used_memory} bytes / {total_memory} bytes ({memory_percent}%)")
                # Log the count of processed change streams
                log.info(f"Processed Change Streams: {c}")
    except OperationFailure as e:
        log.error(f"Error setting up change stream for collection '{collection.name}': {e}")
    except Exception as e:
        log.error(f"Error watching collection: {str(e)}")


def print_change_stream():
    try:
        username = quote_plus(os.getenv('MONGO_USERNAME'))
        password = quote_plus(os.getenv('MONGO_PASSWORD'))
        uri = f"mongodb://{username}:{password}@mongo.libertyfrac.com:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=true"

        client = MongoClient(uri)
        db = client.ENGStageData
        resume_token_collection = db['resume_tokens']

        threads = []
        test_collections = ['Sandbox_Material_boxes', 'resume_tokens', 'Sandbox_TimeTracker_StageData',
                            'Sandbox_TechSheet_StageData', 'Sandbox_TS_Row', 'Test', 'sf_test']
        collections = [collection_name for collection_name in db.list_collection_names() if
                       collection_name not in test_collections]
        event = threading.Event()

        for collection_name in collections:
            collection = db[collection_name]
            thread = threading.Thread(target=watch_collection, args=(collection, resume_token_collection, event))
            threads.append(thread)
            thread.start()

        event.set()  # Set the event to start the change stream in all threads

        for thread in threads:
            thread.join()

        client.close()
    except Exception as e:
        log.error(f"Error in print_change_stream: {str(e)}")


print_change_stream()
