
print('this is modules/aws_ __init__.py')

def send_to_aws_s3_path(data, file_path, accessParams):
    """ 데이터를 AWS S3에 전송하는 함수 """
    import boto3

    # AWS S3에 접근하기 위한 클라이언트를 생성한다.
    s3 = boto3.resource('s3', aws_access_key_id=accessParams['aws_access_key_id'], aws_secret_access_key=accessParams['aws_secret_access_key'])

    # AWS S3에 파일을 저장한다.
    s3.Object(accessParams['aws_s3_bucket_name'], file_path).put(Body=data)

# [x] : SG aws 접속
def SG_s3_connection(aws_access_key_id, aws_secret_access_key):
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