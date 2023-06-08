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

def transform(date, item, grade):
    '''
    날짜를 입력하면 그 날짜의 모든 고구마 거래를 품목별 법인별로 집계하여\n
    특(1등) 등급만 분류하여 단일 json 데이터로 반환하는 함수
    '''
    import math
    import time
    import random

    corp_list = ['11000101','11000102','11000103','11000104','11000105','11000106']

    dict1 = {'data': []} # TODO sg : dict1이 불필요해졌으므로 dict1 과 flatten 부분 수정 필요
    dict2 = {f'{item}': []}
    for corp in corp_list:
        time.sleep(random.uniform(2,4))
        dict3 = {f'{corp}': []}
        list_total_count = int(extract('1', date, corp, item)['lists']['list_total_count'])
        total_page = math.ceil(int(list_total_count) / 10)
        if int(list_total_count) != 0:
            for page in range(1, total_page+1):
                html_dict = extract(page, date, corp, item)
                if list_total_count % 10 > 1:
                    for i in range(len(html_dict['lists']['list'])):
                        if html_dict['lists']['list'][i]['DDD'] == f'{grade}':
                            dict3[f'{corp}'].append({
                                'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT'],
                                'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                'UUN' : html_dict['lists']['list'][i]['UUN'],
                                'DDD' : html_dict['lists']['list'][i]['DDD'],
                                'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                })
                elif list_total_count % 10 == 1:
                    if list_total_count > 1:
                        for i in range(10):
                            if html_dict['lists']['list'][i]['DDD'] == f'{grade}':
                                dict3[f'{corp}'].append({
                                    'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT'],
                                    'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                    'UUN' : html_dict['lists']['list'][i]['UUN'],
                                    'DDD' : html_dict['lists']['list'][i]['DDD'],
                                    'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                    })
                        list_total_count -= 10
                    elif list_total_count == 1:
                        if html_dict['lists']['list']['DDD'] == f'{grade}':
                            dict3[f'{corp}'].append({
                                'ADJ_DT' : html_dict['lists']['list']['ADJ_DT'],
                                'PUMMOK' : html_dict['lists']['list']['PUMMOK'],
                                'UUN' : html_dict['lists']['list']['UUN'],
                                'DDD' : html_dict['lists']['list']['DDD'],
                                'PPRICE' : html_dict['lists']['list']['PPRICE'],
                                })
            dict2[f'{item}'].append(dict3)
        else:
            pass
    dict1['data'].append(dict2)
        
    flattened_data = []
    for item_data in dict1['data']:
        for _, corp_list in item_data.items():
            for corp_data in corp_list:
                for corp, transactions in corp_data.items():
                    for transaction in transactions:
                        flattened_row = transaction.copy()
                        flattened_data.append(flattened_row)

    return flattened_data

# print(transform('20230401', '고구마', '특(1등)'))

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

def load(data, item, grade):
    '''
    파티셔닝 후 저장하는 함수
    '''
    import json
    import os
    import gzip
    from dotenv import load_dotenv
    load_dotenv()

    if len(data) != 0:
        year = data[0]['ADJ_DT'][0:4]
        month = data[0]['ADJ_DT'][4:6]
        date = data[0]['ADJ_DT']

        directory = f'items/{year}/{month}/{item}_{grade}/{date}.json.gz'
        compressed_data = gzip.compress(json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8'))
        
        s3 = s3_connection()
        aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')
        s3.put_object(
            Bucket = aws_s3_bucket_name,
            Body = compressed_data,
            Key = directory,
        )

def etl_pipeline(date1, date2, item, grade):
    from datetime import datetime, timedelta
    start = date1
    end = date2

    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')
    dates = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date-start_date).days+1)]

    for date in dates:
        print(date)
        data = transform(date, item, grade)
        load(data, item, grade)

etl_pipeline('20230601','20230607', '고구마', '특(1등)')

# TODO sg : items/AIModel/[결과데이터 파일].gz 형식으로 저장되게끔 load 함수 수정 / 더미 텍스트 파일로