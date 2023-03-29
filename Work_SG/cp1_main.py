
# 암호화
# 파싱된 데이터 복호화
def decrypt(key, data):
    from cryptography.fernet import Fernet
    import json

    fernet = Fernet(key)
    for i in range(len(data)):
        temp = fernet.decrypt(data[i]['data']).decode('utf-8').replace("'", "\"")
        data[i]['data'] = json.loads(temp)
    return data

# 변환
# 복호화된 데이터 문자열 압축
def convert_data(data):
    from b64uuid import B64UUID
    import re

    for i in range(len(data)):
        user_id = data[i]['data']['user_id']
        short_id = B64UUID(user_id[:32]).string + B64UUID(user_id[32:]).string
        data[i]['data']['user_id'] = short_id

        method = data[i]['data']['method']
        if method == 'GET':
            data[i]['data']['method'] = 1
        elif method == 'POST':
            data[i]['data']['method'] = 2
        elif method == 'PUT':
            data[i]['data']['method'] = 3
        else:
            data[i]['data']['method'] = 4

        url = data[i]['data']['url']
        if url == '/api/products/product/':
            data[i]['data']['url'] = 1
        else:
            data[i]['data']['url'] = 0

        indate = data[i]['data']['inDate']
        data[i]['data']['inDate'] = re.sub("[^0-9]","",indate[2:])

    return data

# 변환(시간)
# 데이터 나누기
def data_split(data):
    first_hour = data[0]['data']['inDate'][6:8]
    date_split = [0]

    for i in range(len(data)):
        next_hour = data[i]['data']['inDate'][6:8]
        if first_hour != next_hour:
            first_hour = next_hour
            date_split.append(i)
    date_split.append(100)

    result = []
    for i in range(len(date_split)-1):
        result.append(data[date_split[i]:date_split[i+1]])
    return result

# 프로젝트 - 패스 설정, 데이터 변환
def set_path_and_data(data):
    """
    데이터 변환 \n
    data: list
    """
    import json
    import gzip

    year = data[0][0]['data']['inDate'][0:2]
    month = data[0][0]['data']['inDate'][2:4]
    day = data[0][0]['data']['inDate'][4:6]
    hour = data[0][0]['data']['inDate'][6:8]
    minutes = data[0][0]['data']['inDate'][8:]

    save_directory = f'{year}/{month}/{day}/{hour}/{minutes}.json.gz'
    compressed_data = gzip.compress(json.dumps(data).encode('utf-8'))
    return save_directory, compressed_data

# aws 연결
def s3_connection(aws_access_key_id, aws_secret_access_key):
    import boto3

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

# 주요 기능
def etl_pipeline(env):
    import requests
    import json

    page = requests.get(env['api_url'])
    parsed_data = json.loads(page.text)

    decrypt_data = decrypt(env['api_key'], parsed_data)
    zip_data = convert_data(decrypt_data)
    splited_data = data_split(zip_data)
    save_directory, compressed_data = set_path_and_data(splited_data)

    s3 = s3_connection(env['aws_access_key_id'], env['aws_secret_access_key'])
    s3.put_object(
        Bucket = env['aws_s3_bucket_name'],
        Body = compressed_data,
        Key = save_directory,
    )


# 외부에서 사용하는 공개 함수
def schedule_etl(env, interval_minutes=5):
    """ 
    etl_pipeline 함수를 주기적으로 실행하는 함수 \n
    args : interval_minutes: 분 단위 
    """
    import schedule
    import time

    schedule.every(interval_minutes).minutes.do(etl_pipeline, env)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
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

    schedule_etl(env, 5)