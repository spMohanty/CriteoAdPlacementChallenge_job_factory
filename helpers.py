#!/usr/bin/env python

import boto3
from config import Config
import requests

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
    # r = requests.put(url, data=open("example_data"))
