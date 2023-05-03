import time
start = time.time()

def get_obj(date):
    '''
    aws S3 에서 파일을 불러온 뒤\n
    gzip 압축을 풀고 json 데이터를 반환하는 함수
    '''
    import boto3
    import os
    import gzip
    import json
    from dotenv import load_dotenv
    load_dotenv()

    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')

    s3 = boto3.client(
                service_name="s3",
                region_name="ap-northeast-2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )

    year = date[0:4]
    month = date[4:6]

    obj = s3.get_object(
        Bucket = aws_s3_bucket_name,
        Key = f'{year}/{month}/{date}.json.gz'
    )

    with gzip.GzipFile(fileobj=obj.get('Body'), mode='r') as gz:
        content = gz.read()

    json_data = json.loads(content)
    return json_data

json_data = get_obj('20230426')

def save_local(data):
    '''
    불러온 json 데이터를 로컬에 저장하는 함수
    '''
    import json
    
    path = 'CP2/20230426.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

save_local(json_data)

end = time.time()
print(f"{end - start:.2f} sec")
# 1.03초 소요