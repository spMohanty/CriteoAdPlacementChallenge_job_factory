#!/usr/bin/env python

import boto3
from config import Config
import requests
import os

def obtain_presigned_url(filename):
    s3 = boto3.client(  's3',
                        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
                        )
    file_key = "{}/{}".format(Config.S3_UPLOAD_PATH, filename)
    # TODO: Add content length limits, etc
    url = s3.generate_presigned_url(
                        'put_object',
                        Params  =   {
                            'Bucket':Config.AWS_S3_BUCKET,
                            'Key': file_key
                            },
                        ExpiresIn=3600,
                        HttpMethod='PUT')
    return file_key, url

def download_file_from_s3(key):
    if not os.path.exists(Config.TEMP_STORAGE_DIRECTORY_PATH):
        os.mkdir(Config.TEMP_STORAGE_DIRECTORY_PATH)

    s3 = boto3.client(  's3',
                        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
                        )

    #Try to download file locally
    file_path = "{}/{}".format(Config.TEMP_STORAGE_DIRECTORY_PATH, key.split("/")[-1])
    s3.download_file(Config.AWS_S3_BUCKET, key, file_path)
    return "{}/{}".format(Config.TEMP_STORAGE_DIRECTORY_PATH, key.split("/")[-1])
