from Core import Core

class ETL_SG(Core):
    """
    SG sub_project etl pipeline
    """

    def _set_extract_param(self, page, date, bubin, pummok):
        """
        extract_url 함수의 param을 설정하는 함수
        """
        import os
        from dotenv import load_dotenv
        load_dotenv()

        API_ID = os.getenv('garak_id')
        API_PW = os.getenv('garak_passwd')

        params = (
                    ('id', API_ID),
                    ('passwd', API_PW),
                    ('dataid', 'data12'),
                    ('pagesize', '10'),
                    ('pageidx', page),
                    ('portal.templet', 'false'),
                    ('s_date', date),
                    ('s_bubin', bubin),
                    ('s_pummok', pummok),
                    ('s_sangi', '')
                 )

        return params

    def extract_url(self, url, param = None):
        import cp2_modules.extract_ as ext

        return ext.extract(url, param)

    def _extract_data(self, date, bubin_list, pummok_list):
        """
        특수한 경우,
        여러번의 _extract_url 함수 호출로 전체 데이터를 추출
        """
        import math
        # import json
        import pandas as pd

        # region BLOCK 1: set dict1
        dict1 = {'data': []}
        for pummok in pummok_list:
            dict2 = {f'{pummok}': []}
            for bubin in bubin_list:

                #region get total count

                params = self._set_extract_param(1, date, bubin, pummok)

                dict3 = {f'{bubin}': []}
                # extract url 1
                list_total_count = int(self.extract_url(self.env['URL'], params)['lists']['list_total_count'])
                total_page = math.ceil(int(list_total_count) / 10)

                #endregion

                if int(list_total_count) == 0:
                    continue
                
                #region set data per page

                for page in range(1, total_page+1):

                    params = self._set_extract_param(page, date, bubin, pummok)
                    # extract url 2
                    html_dict = self.extract_url(self.env['URL'], params)

                    _data = {
                        'idx' : ((page -1) * 10) + (i + 1),
                        'PUMMOK' : html_dict['lists']['list'][i]['PUMMOK'],
                        'PUMJONG' : html_dict['lists']['list'][i]['PUMJONG'],
                        'UUN' : html_dict['lists']['list'][i]['UUN'],
                        'DDD' : html_dict['lists']['list'][i]['DDD'],
                        'PPRICE' : html_dict['lists']['list'][i]['PPRICE'],
                        'SSANGI' : html_dict['lists']['list'][i]['SSANGI'],
                        'CORP_NM' : html_dict['lists']['list'][i]['CORP_NM'],
                        'ADJ_DT' : html_dict['lists']['list'][i]['ADJ_DT']
                    }

                    if list_total_count % 10 > 1:
                        for i in range(len(html_dict['lists']['list'])):
                            dict3[f'{bubin}'].append(_data)
                    elif list_total_count % 10 == 1:
                        if list_total_count > 1:
                            for i in range(10):
                                dict3[f'{bubin}'].append(_data)
                            list_total_count -= 10
                        elif list_total_count == 1:
                            _data['idx'] = int(html_dict['lists']['list_total_count'])
                            dict3[f'{bubin}'].append(_data)

                dict2[f'{pummok}'].append(dict3)

                #endregion

            dict1['data'].append(dict2)

        # endregion

        return dict1
    
    def _transform_data(self, data, bubin_list, pummok_list):
        # region BLOCK 2: set flattened_data
        flattened_data = []
        for item_data in data['data']:
            for item, bubin_list in item_data.items():
                for bubin_data in bubin_list:
                    for bubin, transactions in bubin_data.items():
                        for transaction in transactions:
                            flattened_row = transaction.copy()
                            flattened_row['item'] = item
                            flattened_row['bubin'] = bubin
                            flattened_data.append(flattened_row)

        # endregion

        return flattened_data


    def _load_data(self, data, partitioning_func = None):
        """
        Spark 를 이용한 데이터 처리 및 분석 가능
        (DataFrame 형태로 반환)
        """
        import cp2_modules.load_ as aws

        aws.s3_load(data, self.env['AWS_SERVICE_NAME'], self.env['REGION'], self.env['AWS_ACCESS_ID'], 
                    self.env['AWS_SECRET_KEY'], self.env['AWS_BUCKET_NAME'], partitioning_func)


    def partitioning(self, flattened_data):
        if len(flattened_data) != 0:
            year = flattened_data[0]['ADJ_DT'][0:4]
            month = flattened_data[0]['ADJ_DT'][4:6]
            date = flattened_data[0]['ADJ_DT']

            directory = f'{year}/{month}/{date}.json.gz'

            return flattened_data, directory

    def etl_stream(self, date):
        import math
        import json
        import pandas as pd
        from dotenv import load_dotenv
        load_dotenv()

        # self.env['bubin_list'] = ['11000101','11000102','11000103','11000104','11000105','11000106']

        bubin_list = ['11000101','11000102','11000103','11000104','11000105','11000106']
        pummok_list = ['감귤','감자','건고추','고구마','단감','당근','딸기','마늘','무',
                        '미나리','바나나','배','배추','버섯','사과','상추','생고추','수박',
                        '시금치','양배추','양상추','양파','오이','참외','토마토','파',
                        '포도','피망','호박']
        
        dict1 = self._extract_data(date, bubin_list, pummok_list)

        flattened_data = self._transform_data(dict1, bubin_list, pummok_list)

        # schema 설정
        # sparkSession open

        # spark 처리 코드 작성가능

        self._load_data(flattened_data, self.partitioning)

    def run_etl(self):
        self.etl_stream(self['TARGET_DATE'])

        pass
    