
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

def transform(date):
    '''
    날짜를 입력하면 그 날짜의 모든 거래를 품목별 법인별로 집계하여\n
    단일 json 데이터로 반환하는 함수
    '''
    import math

    bubin_list = ['11000101','11000102','11000103','11000104','11000105','11000106']
    pummok_list = ['고구마']

    dict1 = {'data': []}
    for pummok in pummok_list:
        dict2 = {f'{pummok}': []}
        for bubin in bubin_list:
            dict3 = {f'{bubin}': []}
            list_total_count = int(extract('1', date, bubin, pummok)['lists']['list_total_count'])
            total_page = math.ceil(int(list_total_count) / 10)
            if int(list_total_count) != 0:
                for page in range(1, total_page+1):
                    html_dict = extract(page, date, bubin, pummok)
                    if list_total_count % 10 > 1:
                        for i in range(len(html_dict['lists']['list'])):
                            dict3[f'{bubin}'].append({
                                'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT'],
                                'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                'PUMJONG' : html_dict['lists']['list'][i]['PUMJONG'],
                                'UUN' : html_dict['lists']['list'][i]['UUN'],
                                'DDD' : html_dict['lists']['list'][i]['DDD'],
                                'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                'SSANGI' : html_dict['lists']['list'][i]['SSANGI'],
                                'CORP_NM' : html_dict['lists']['list'][i]['CORP_NM'],
                                })
                    elif list_total_count % 10 == 1:
                        if list_total_count > 1:
                            for i in range(10):
                                dict3[f'{bubin}'].append({
                                    'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT'],
                                    'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                    'PUMJONG' : html_dict['lists']['list'][i]['PUMJONG'],
                                    'UUN' : html_dict['lists']['list'][i]['UUN'],
                                    'DDD' : html_dict['lists']['list'][i]['DDD'],
                                    'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                    'SSANGI' : html_dict['lists']['list'][i]['SSANGI'],
                                    'CORP_NM' : html_dict['lists']['list'][i]['CORP_NM'],
                                    })
                            list_total_count -= 10
                        elif list_total_count == 1:
                            dict3[f'{bubin}'].append({
                                'ADJ_DT' : html_dict['lists']['list']['ADJ_DT'],
                                'PUMMOK' : html_dict['lists']['list']['PUMMOK'],
                                'PUMJONG' : html_dict['lists']['list']['PUMJONG'],
                                'UUN' : html_dict['lists']['list']['UUN'],
                                'DDD' : html_dict['lists']['list']['DDD'],
                                'PPRICE' : html_dict['lists']['list']['PPRICE'],
                                'SSANGI' : html_dict['lists']['list']['SSANGI'],
                                'CORP_NM' : html_dict['lists']['list']['CORP_NM'],
                                })
                dict2[f'{pummok}'].append(dict3)
            else:
                pass
        dict1['data'].append(dict2)
        
    return dict1

    # flattened_data = []
    # for item_data in dict1['data']:
    #     for item, bubin_list in item_data.items():
    #         for bubin_data in bubin_list:
    #             for bubin, transactions in bubin_data.items():
    #                 for transaction in transactions:
    #                     flattened_row = transaction.copy()
    #                     flattened_row['bubin'] = bubin
    #                     flattened_data.append(flattened_row)

    # return flattened_data

print(transform('20230401'))

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

def load(data):
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

        directory = f'goguma/{year}/{month}/{date}.json.gz'
        compressed_data = gzip.compress(json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8'))
        
        s3 = s3_connection()
        aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')
        s3.put_object(
            Bucket = aws_s3_bucket_name,
            Body = compressed_data,
            Key = directory,
        )

def etl_pipeline(date1, date2):
    from datetime import datetime, timedelta
    start = date1
    end = date2

    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')
    dates = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date-start_date).days+1)]

    for date in dates:
        print(date)
        data = transform(date)
        load(data)

# etl_pipeline('20230401','20230430')