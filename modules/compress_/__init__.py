
print('this is modules/compress_ __init__.py')

# TODO : json 모듈과 연결성 나중에 생각해보기
def _json_dump_data(data):
    """ 데이터를 파일로 저장하는 함수 """
    import json

    return json.dumps(data)

def compress_data(str_data):
    """ 데이터를 압축하는 함수 """
    import zlib
    return zlib.compress(str_data.encode())

    # import gzip
    # return gzip.compress(str_data.encode())

def compress_dict(dict_data):
    """ dict 데이터를 압축하는 함수 """
    return compress_data(_json_dump_data(dict_data))

# [x] : SG 복호화 함수
def SG_decrypt(key, data):
    from cryptography.fernet import Fernet
    import json

    fernet = Fernet(key)
    for i in range(len(data)):
        temp = fernet.decrypt(data[i]['data']).decode('utf-8').replace("'", "\"")
        data[i]['data'] = json.loads(temp)
    return data