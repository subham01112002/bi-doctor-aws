import boto3
import os
from botocore.exceptions import BotoCoreError, ClientError

s3 = boto3.client("s3")

def upload_excel_to_s3(local_file_path: str, bucket: str, key_prefix: str = "exports"):
    try:
        filename = os.path.basename(local_file_path)
        s3_key = f"{key_prefix}/{filename}"

        s3.upload_file(
            Filename=local_file_path,
            Bucket=bucket,
            Key=s3_key
        )
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        return s3_key

    except FileNotFoundError as e:
        raise RuntimeError(f"Local file not found: {local_file_path}") from e

    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to upload file to S3 bucket '{bucket}'") from e

    except Exception as e:
        raise RuntimeError("Unexpected error occurred during S3 upload") from e
    finally:        
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
