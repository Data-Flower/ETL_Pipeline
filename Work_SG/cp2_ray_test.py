import time
import ray
from datetime import datetime, timedelta, date

time_start = time.time()

# ray.init(num_cpus=16)

# @ray.remote
def etl(date):
    import os
    import requests
    import boto3
    import math
    import xmltodict
    import json
    import gzip
    from datetime import datetime, timedelta
    from dotenv import load_dotenv
    load_dotenv()

    garak_id = os.environ.get('garak_id')
    garak_passwd = os.environ.get('garak_passwd')
    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')

    bubin_list = ['11000101','11000102','11000103','11000104','11000105','11000106']
    pummok_list = ['감귤','감자','건고추','고구마','단감','당근','딸기','마늘','무',
                '미나리','바나나','배','배추','버섯','사과','상추','생고추','수박',
                '시금치','양배추','양상추','양파','오이','참외','토마토','파',
                '포도','피망','호박']

    dict1 = {'data': []}
    for pummok in pummok_list:
        dict2 = {f'{pummok}': []}
        for bubin in bubin_list:
            dict3 = {f'{bubin}': []}
            url = 'http://www.garak.co.kr/publicdata/dataOpen.do?'
            params = (
                ('id', garak_id),
                ('passwd', garak_passwd),
                ('dataid', 'data12'),
                ('pagesize', '10'),
                ('pageidx', '1'),
                ('portal.templet', 'false'),
                ('s_date', date),
                ('s_bubin', bubin),
                ('s_pummok', pummok),
                ('s_sangi', '')
            )

            response = requests.get(url,params=params)
            html = response.text
            html_dict = xmltodict.parse(html)

            list_total_count = int(html_dict['lists']['list_total_count'])
            total_page = math.ceil(int(list_total_count) / 10)
            if int(list_total_count) != 0:
                for page in range(1, total_page+1):
                    url = 'http://www.garak.co.kr/publicdata/dataOpen.do?'
                    params = (
                        ('id', garak_id),
                        ('passwd', garak_passwd),
                        ('dataid', 'data12'),
                        ('pagesize', '10'),
                        ('pageidx', page),
                        ('portal.templet', 'false'),
                        ('s_date', date),
                        ('s_bubin', bubin),
                        ('s_pummok', pummok),
                        ('s_sangi', '')
                    )

                    response = requests.get(url,params=params)
                    html = response.text
                    html_dict = xmltodict.parse(html)

                    if list_total_count % 10 > 1:
                        for i in range(len(html_dict['lists']['list'])):
                            dict3[f'{bubin}'].append({
                                'idx' : ((page -1) * 10) + (i + 1),
                                'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                'PUMJONG' : html_dict['lists']['list'][i]['PUMJONG'],
                                'UUN' : html_dict['lists']['list'][i]['UUN'],
                                'DDD' : html_dict['lists']['list'][i]['DDD'],
                                'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                'SSANGI' : html_dict['lists']['list'][i]['SSANGI'],
                                'CORP_NM' : html_dict['lists']['list'][i]['CORP_NM'],
                                'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT']
                                })
                    elif list_total_count % 10 == 1:
                        if list_total_count > 1:
                            for i in range(10):
                                dict3[f'{bubin}'].append({
                                    'idx' : ((page -1) * 10) + (i + 1),
                                    'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                                    'PUMJONG' : html_dict['lists']['list'][i]['PUMJONG'],
                                    'UUN' : html_dict['lists']['list'][i]['UUN'],
                                    'DDD' : html_dict['lists']['list'][i]['DDD'],
                                    'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                                    'SSANGI' : html_dict['lists']['list'][i]['SSANGI'],
                                    'CORP_NM' : html_dict['lists']['list'][i]['CORP_NM'],
                                    'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT']
                                    })
                            list_total_count -= 10
                        elif list_total_count == 1:
                            dict3[f'{bubin}'].append({
                                'idx' : int(html_dict['lists']['list_total_count']),
                                'PUMMOK' : html_dict['lists']['list']['PUMMOK'],
                                'PUMJONG' : html_dict['lists']['list']['PUMJONG'],
                                'UUN' : html_dict['lists']['list']['UUN'],
                                'DDD' : html_dict['lists']['list']['DDD'],
                                'PPRICE' : html_dict['lists']['list']['PPRICE'],
                                'SSANGI' : html_dict['lists']['list']['SSANGI'],
                                'CORP_NM' : html_dict['lists']['list']['CORP_NM'],
                                'ADJ_DT' : html_dict['lists']['list']['ADJ_DT']
                                })
                dict2[f'{pummok}'].append(dict3)
            else:
                pass
        dict1['data'].append(dict2)

    flattened_data = []
    for item_data in dict1['data']:
        for item, bubin_list in item_data.items():
            for bubin_data in bubin_list:
                for bubin, transactions in bubin_data.items():
                    for transaction in transactions:
                        flattened_row = transaction.copy()
                        flattened_row['item'] = item
                        flattened_row['bubin'] = bubin
                        flattened_data.append(flattened_row)

    s3 = boto3.client(
    service_name="s3",
    region_name="ap-northeast-2",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    )

    if len(flattened_data) != 0:
        year = flattened_data[0]['ADJ_DT'][0:4]
        month = flattened_data[0]['ADJ_DT'][4:6]
        date = flattened_data[0]['ADJ_DT']

        directory = f'{year}/{month}/{date}.json.gz'
        compressed_data = gzip.compress(json.dumps(flattened_data, ensure_ascii=False, indent=4).encode('utf-8'))
        
        aws_s3_bucket_name = os.environ.get('aws_s3_bucket_name')
        s3.put_object(
            Bucket = aws_s3_bucket_name,
            Body = compressed_data,
            Key = directory,
        )

start = '20221226'
end = '20221231'

start_date = datetime.strptime(start, "%Y%m%d")
end_date = datetime.strptime(end, "%Y%m%d")

arr = []
while start_date <= end_date:
    dates = start_date.strftime('%Y%m%d')
    arr.append(dates)
    start_date += timedelta(days=1)

# @ray.remote
def etl_pipeline(start, end):
    from datetime import datetime, timedelta

    start_date = datetime.strptime(start, "%Y%m%d")
    end_date = datetime.strptime(end, "%Y%m%d")

    arr = []
    while start_date <= end_date:
        dates = start_date.strftime('%Y%m%d')
        arr.append(dates)
        start_date += timedelta(days=1)

    for i in arr:
        etl(i)

# for i in arr:
#     ray.get(etl.remote(i))

# ray.shutdown()

etl_pipeline('20221226', '20221231')

time_end = time.time()
print(f"{time_end - time_start:.2f} sec")
# 1614.56