from ETL_SG import ETL_SG
# from pyspark.sql.types import *
# from pyspark.sql import SparkSession


AWS_SERVICE_NAME = 's3'
REGION = "ap-northeast-2"

AWS_ACCESS_ID = 'aws_access_key_id'
AWS_SECRET_KEY = 'aws_secret_access_key'
AWS_BUCKET_NAME = 'aws_s3_bucket_name'

TARGET_DATE = '20230407'

env = {
    'AWS_SERVICE_NAME': AWS_SERVICE_NAME,
    'REGION': REGION,
    'AWS_ACCESS_ID': AWS_ACCESS_ID,
    'AWS_SECRET_KEY': AWS_SECRET_KEY,
    'AWS_BUCKET_NAME': AWS_BUCKET_NAME,
    'TARGET_DATE': TARGET_DATE,
    'URL': 'http://www.garak.co.kr/publicdata/dataOpen.do?'
}

etl = ETL_SG(env)
etl.run()

        
# etl_stream('20230407')

# spark.stop()
    