def s3_connection():
    '''
    aws S3에 연결하는 함수
    '''
    import os
    import boto3
    from dotenv import load_dotenv
    load_dotenv()

    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')

    try:
        s3 = boto3.client(
            service_name="s3",
            region_name="ap-northeast-2",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    except Exception as e:
        print(e)
    else:
        print("s3 bucket connected!") 
        return s3
    
def partitioning(data):
    '''
    파티셔닝 함수
    '''
    if len(data['data']) != 0:
        year = data[0]['ADJ_DT'][0:4]
        month = data[0]['ADJ_DT'][4:6]
        date = data[0]['ADJ_DT']

        directory = f'{year}/{month}/{date}.json.gz'
    
    return directory

def compress(data):
    '''
    gzip으로 압축하는 함수
    '''
    import json
    import gzip

    compressed_data = gzip.compress(json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8'))
    return compressed_data

def load(data):
    '''
    S3에 저장하는 함수
    '''
    import os
    from modules import load_
    from dotenv import load_dotenv
    load_dotenv()

    s3 = load_.s3_connection()
    compressed_data = load_.compress(data)
    directory = load_.load(data)

    aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')
    s3.put_object(
        Bucket = aws_s3_bucket_name,
        Body = compressed_data,
        Key = directory,
    )