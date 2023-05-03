def data_format1(page, data, i):
    format = {'idx' : ((page -1) * 10) + (i + 1),
            'PUMMOK' : data['lists']['list'][i]['PUMMOK'],
            'PUMJONG' : data['lists']['list'][i]['PUMJONG'],
            'UUN' : data['lists']['list'][i]['UUN'],
            'DDD' : data['lists']['list'][i]['DDD'],
            'PPRICE' : data['lists']['list'][i]['PPRICE'],
            'SSANGI' : data['lists']['list'][i]['SSANGI'],
            'CORP_NM' : data['lists']['list'][i]['CORP_NM'],
            'ADJ_DT' : data['lists']['list'][i]['ADJ_DT']
            }
    return format

def data_format2(data):
    format = {'idx' : int(data['lists']['list_total_count']),
            'PUMMOK' : data['lists']['list']['PUMMOK'],
            'PUMJONG' : data['lists']['list']['PUMJONG'],
            'UUN' : data['lists']['list']['UUN'],
            'DDD' : data['lists']['list']['DDD'],
            'PPRICE' : data['lists']['list']['PPRICE'],
            'SSANGI' : data['lists']['list']['SSANGI'],
            'CORP_NM' : data['lists']['list']['CORP_NM'],
            'ADJ_DT' : data['lists']['list']['ADJ_DT']
            }
    return format

def integrated_data(date, bubin, pummok):
    import math
    from modules import parameter_, extract_

    url = parameter_.url()
    params1 = parameter_.page_param(page='1', s_date=date, s_bubin=bubin, s_pummok=pummok)

    list_total_count = int(extract_.extract(url, params1)['lists']['list_total_count'])
    total_page = math.ceil(int(list_total_count) / 10)

    dict = {f'{bubin}': []}
    if int(list_total_count) != 0:
        for page in range(1, total_page+1):
            params2 = parameter_.page_param(page=page, s_date=date, s_bubin=bubin, s_pummok=pummok)
            html_dict = extract_.extract(url, params2)
            if list_total_count % 10 > 1:
                for i in range(len(html_dict['lists']['list'])):
                    dict[f'{bubin}'].append(data_format1(page, html_dict, i))
            elif list_total_count % 10 == 1:
                if list_total_count > 1:
                    for i in range(10):
                        dict[f'{bubin}'].append(data_format1(page, html_dict, i))
                    list_total_count -= 10
                elif list_total_count == 1:
                    dict[f'{bubin}'].append(data_format2(html_dict))
    else:
        pass
    return dict

def flatten(dict):
    flattened_data = []
    for item_data in dict['data']:
        for item, bubin_list in item_data.items():
            for bubin_data in bubin_list:
                for bubin, transactions in bubin_data.items():
                    for transaction in transactions:
                        flattened_row = transaction.copy()
                        flattened_row['item'] = item
                        flattened_row['bubin'] = bubin
                        flattened_data.append(flattened_row)

    return flattened_data

def transform(date):
    '''
    날짜를 입력하면 그 날짜의 모든 거래를 품목별 법인별로 집계하여\n
    단일 json 데이터로 반환하는 함수
    '''
    from modules import parameter_

    bubin_list, pummok_list = parameter_.lists()

    dict1 = {'data': []}
    for pummok in pummok_list:
        dict2 = {f'{pummok}': []}
        for bubin in bubin_list:
            dict3 = integrated_data(date=date, bubin=bubin, pummok=pummok)
            dict2[f'{pummok}'].append(dict3)
        dict1['data'].append(dict2)
        
    data = flatten(dict1)
    return data