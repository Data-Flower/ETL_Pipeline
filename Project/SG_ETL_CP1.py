from Core import Core

class SG_ETL_CP1(Core):
    def __init__(self, env):
        super().__init__(env)

    def _extract_url(self, url):
        import requests
        import json

        page = requests.get(url)
        parsed_data = json.loads(page.text)
        return parsed_data

    def _transform_data(self, data):
        from modules.compress_ import SG_decrypt
        from modules.converts_ import SG_convert_data
        from modules.times_ import SG_data_split

        decrypt_data = SG_decrypt(self.env['api_key'], data)
        zip_data = SG_convert_data(decrypt_data)
        splited_data = SG_data_split(zip_data)
        save_directory, compressed_data = self.set_path_and_data(splited_data)
        return save_directory, compressed_data

    def _load_data(self, data, filepath):
        from modules.aws_ import SG_s3_connection

        s3 = SG_s3_connection(self.env['aws_access_key_id'], self.env['aws_secret_access_key'])
        s3.put_object(
            Bucket = self.env['aws_s3_bucket_name'],
            Body = data,
            Key = filepath,
        )
    
    def _set_path_and_data(self, data):
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


    def _etl_pipeline(self):
        # Extract 추출
        parsed_data = self._extract_url(self.env['api_url'])

        # Transform 변환
        save_directory, compressed_data = self._transform_data(parsed_data)

        # Load 적재
        self._load_data(compressed_data, save_directory)

    def _schedule_job(self, interval_minutes=5):
        """ 
        etl_pipeline 함수를 주기적으로 실행하는 함수 \n
        args : interval_minutes: 분 단위 
        """
        import schedule
        import time

        schedule.every(interval_minutes).minutes.do(self._etl_pipeline)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def run(self, interval_minutes=5):
        self._schedule_job(interval_minutes)