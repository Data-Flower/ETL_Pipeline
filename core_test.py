
# from Project.Core import Core

# env_test = {
#     "env" : "test",
#     "url" : "localhost:8080",
#     "aws" : {
#         "aws_access_key_id":"aws_key",
#         "aws_secret_access_key":"aws_secret",
#         "aws_s3_bucket_name":"my_bucket"
#     }
# }

# core = Core("aaa")
# core.extract_url("aaa")

# -----

# from Project.ETL_CP1 import ETL_CP1

# etl = ETL_CP1(env_test)
# etl.run(interval_minutes=1)

# -----

import os
from dotenv import load_dotenv
load_dotenv()

api_url = 'http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log'
api_key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='

# aws s3 연결
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')

env = {
    'api_url': api_url,
    'api_key': api_key,
    'aws_access_key_id': aws_access_key_id,
    'aws_secret_access_key': aws_secret_access_key,
    'aws_s3_bucket_name': aws_s3_bucket_name
}

from Project.SG_ETL_CP1 import SG_ETL_CP1

etl = SG_ETL_CP1(env)
etl.run(interval_minutes=5)