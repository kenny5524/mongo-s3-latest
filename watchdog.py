import time
import sched
import boto3
from dotenv import load_dotenv
import os
import glob
from datetime import datetime

load_dotenv('/home/ec2-user/.env')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')  # replace with your AWS access key ID
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')  # replace with your AWS secret access key
BUCKET_NAME = os.getenv('BUCKET_NAME')  # replace with your target S3 bucket name
UPLOAD_FOLDER = 'stream_logs'  # replace with your desired upload folder path

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def upload_to_s3():
    log_files_path = 'mongodb_stream*.log'  # replace with the actual path of the log files
    log_files = glob.glob(log_files_path)
    for log_file_path in log_files:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_file_path = f"{UPLOAD_FOLDER}/{timestamp}_{os.path.basename(log_file_path)}"
        try:
            s3.upload_file(log_file_path, BUCKET_NAME, s3_file_path)
            print(f"Uploaded log file to S3: s3://{BUCKET_NAME}/{s3_file_path}")
            os.remove(log_file_path)  # remove the file after upload
            print(f"Removed local log file: {log_file_path}")
        except Exception as e:
            print(f"Error uploading or deleting log file '{log_file_path}': {str(e)}")

if __name__ == "__main__":
    load_dotenv('/home/ec2-user/.env')

    scheduler = sched.scheduler(time.time, time.sleep)
    interval = 1860  # interval in seconds (31 minutes)

    while True:
        scheduler.enter(interval, 1, upload_to_s3)
        scheduler.run()


