from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from datetime import datetime

with DAG(
    "etl_pipeline2",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        'wait_for_downstream': True,
    },

    description="ETL Pipeline",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 4, 8),
    catchup=False,
    tags=["ETL"],
) as dag:
    
    def extract():
        import requests
        import xmltodict

        url = 'http://www.garak.co.kr/publicdata/dataOpen.do?'

        params = (
            ('id', '3392'),
            ('passwd', 'tmcltmcl547!'),
            ('dataid', 'data12'),
            ('pagesize', '10'),
            ('pageidx', '1'),
            ('portal.templet', 'false'),
            ('s_date', '20230407'),
            ('s_bubin', '11000101'),
            ('s_pummok', '감귤'),
            ('s_sangi', '')
        )

        response = requests.get(url,params=params)
        html = response.text
        html_dict = xmltodict.parse(html)
        return html_dict

    def transform(**context):
        dictionary = {'data':[]}
        data = context['task_instance'].xcom_pull(task_ids=f'extract')
        for i in range(len(data['lists']['list'])):
            dictionary['data'].append({
                'PUMMOK' : data['lists']['list'][i]['PUMMOK'],
                'PUMJONG' : data['lists']['list'][i]['PUMJONG'],
                'UUN' : data['lists']['list'][i]['UUN'],
                'DDD' : data['lists']['list'][i]['DDD'],
                'PPRICE' : data['lists']['list'][i]['PPRICE'],
                'SSANGI' : data['lists']['list'][i]['SSANGI'],
                'CORP_NM' : data['lists']['list'][i]['CORP_NM'],
                'ADJ_DT' : data['lists']['list'][i]['ADJ_DT']
                })
        return dictionary

    def load(**context):
        import boto3
        import json
        import gzip
        import os
        from dotenv import load_dotenv
        load_dotenv()

        data = context['task_instance'].xcom_pull(task_ids=f'transform')

        directory = 'test.json.gz'
        compressed_data = gzip.compress(json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8'))
        
        aws_access_key_id = os.environ.get('aws_access_key_id')
        aws_secret_access_key = os.environ.get('aws_secret_access_key')
        
        s3 = boto3.client(
            service_name="s3",
            region_name="ap-northeast-2",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        
        s3.put_object(
            Bucket = 'osg-bucket1',
            Body = compressed_data,
            Key = directory,
        )

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract,
        dag=dag,
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load,
        dag=dag
    )

    t1 >> t2 >> t3