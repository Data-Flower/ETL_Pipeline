def extract(page, s_date, s_bubin, s_pummok):
    '''
    api 데이터 호출 함수
    '''
    import os
    import requests
    import xmltodict
    from dotenv import load_dotenv
    load_dotenv()

    garak_id = os.environ.get('garak_id')
    garak_passwd = os.environ.get('garak_passwd')

    url = 'http://www.garak.co.kr/publicdata/dataOpen.do?'

    params = (
        ('id', garak_id),
        ('passwd', garak_passwd),
        ('dataid', 'data12'),
        ('pagesize', '10'),
        ('pageidx', page),
        ('portal.templet', 'false'),
        ('s_date', s_date),
        ('s_bubin', s_bubin),
        ('s_pummok', s_pummok),
        ('s_sangi', '')
    )

    response = requests.get(url,params=params)
    html = response.text
    html_dict = xmltodict.parse(html)
    return html_dict

def transform(date, pummok, DDD):
    '''
    날짜를 입력하면 그 날짜의 모든 고구마 거래를 품목별 법인별로 집계하여\n
    특(1등) 등급만 분류하여 단일 json 데이터로 반환하는 함수
    '''
    import math
    import time
    import random

    bubin_list = ['11000101','11000102','11000103','11000104','11000105','11000106']
    pummok_list = [f'{pummok}']

    dict1 = {'data': []}
    for pummok in pummok_list:
        dict2 = {f'{pummok}': []}
        for bubin in bubin_list:
            time.sleep(random.uniform(2,4))
            dict3 = {f'{bubin}': []}
            list_total_count = int(extract('1', date, bubin, pummok)['lists']['list_total_count'])
            total_page = math.ceil(int(list_total_count) / 10)
            if int(list_total_count) != 0:
                for page in range(1, total_page+1):
                    html_dict = extract(page, date, bubin, pummok)
                    if list_total_count % 10 > 1:
                        for i in range(len(html_dict['lists']['list'])):
                            if html_dict['lists']['list'][i]['DDD'] == f'{DDD}':
                                dict3[f'{bubin}'].append({
                                    'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT'],
                                    'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                    'UUN' : html_dict['lists']['list'][i]['UUN'],
                                    'DDD' : html_dict['lists']['list'][i]['DDD'],
                                    'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                    })
                    elif list_total_count % 10 == 1:
                        if list_total_count > 1:
                            for i in range(10):
                                if html_dict['lists']['list'][i]['DDD'] == f'{DDD}':
                                    dict3[f'{bubin}'].append({
                                        'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT'],
                                        'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                        'UUN' : html_dict['lists']['list'][i]['UUN'],
                                        'DDD' : html_dict['lists']['list'][i]['DDD'],
                                        'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                        })
                            list_total_count -= 10
                        elif list_total_count == 1:
                            if html_dict['lists']['list']['DDD'] == f'{DDD}':
                                dict3[f'{bubin}'].append({
                                    'ADJ_DT' : html_dict['lists']['list']['ADJ_DT'],
                                    'PUMMOK' : html_dict['lists']['list']['PUMMOK'],
                                    'UUN' : html_dict['lists']['list']['UUN'],
                                    'DDD' : html_dict['lists']['list']['DDD'],
                                    'PPRICE' : html_dict['lists']['list']['PPRICE'],
                                    })
                dict2[f'{pummok}'].append(dict3)
            else:
                pass
        dict1['data'].append(dict2)
        
    flattened_data = []
    for item_data in dict1['data']:
        for _, bubin_list in item_data.items():
            for bubin_data in bubin_list:
                for bubin, transactions in bubin_data.items():
                    for transaction in transactions:
                        flattened_row = transaction.copy()
                        flattened_data.append(flattened_row)

    return flattened_data

print(transform('20230401', '고구마', '특(1등)'))

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

def load_AIModel(aiModel, filename):
    """
    DS 파트에서 작성된 데이터를 S3에 저장하는 함수
    TODO sg : 더미 데이터(.txt) 파일로 테스트 진행

    Parameters
    ----------
    aiModel : var(어떤 변수인지는 모르나 데이터인 것은 확실함)
        DS 파트에서 작성된 데이터
    filename : str
        저장할 파일명

    Returns
    -------
    None.
    """

    import common.compress_
    import os
    from dotenv import load_dotenv
    load_dotenv()

    if aiModel is None:
        raise ValueError("aiModel is None")

    directory = f'items/AIModel/{filename}.gz'    
    compressed_data = common.compress_.compress(aiModel)

    s3 = s3_connection()
    aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')
    s3.put_object(
        Bucket = aws_s3_bucket_name,
        Body = compressed_data,
        Key = directory,
    )


def load(data, pummok): # 
    '''
    파티셔닝 후 저장하는 함수
    '''
    import json
    import os
    import gzip
    from dotenv import load_dotenv
    load_dotenv()

    if len(data) == 0 or data is None:
        raise ValueError("data is None")

    year = data[0]['ADJ_DT'][0:4]
    month = data[0]['ADJ_DT'][4:6]
    date = data[0]['ADJ_DT']

    directory = f'items/{year}/{month}/{pummok}/{date}.json.gz' # TODO sg : 품목_등급으로 폴더명
    compressed_data = gzip.compress(json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8'))
    
    s3 = s3_connection()
    aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')
    s3.put_object(
        Bucket = aws_s3_bucket_name,
        Body = compressed_data,
        Key = directory,
    )

def etl_pipeline(date1, date2, pummok, DDD):
    from datetime import datetime, timedelta
    start = date1
    end = date2

    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')
    dates = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date-start_date).days+1)]

    for date in dates:
        print(date)
        data = transform(date, pummok, DDD)
        load(data, pummok)

etl_pipeline('20221226','20221231', '고구마', '특(1등)')

# TODO sg : items/AIModel/[결과데이터 파일].gz 형식으로 저장되게끔 load 함수 수정 / 더미 텍스트 파일로