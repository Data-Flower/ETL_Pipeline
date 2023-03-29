
print('this is modules/converts_ __init__.py')

def convert_method_to_int(method):
    """ method를 int로 변환하는 함수 """
    if method == 'POST':
        return 1
    elif method == 'GET':
        return 2
    elif method == 'PUT':
        return 3
    elif method == 'DELETE':
        return 4
    else:
        return 0

# [x] : SG 데이터 변환
def SG_convert_data(data):
    from b64uuid import B64UUID
    import re

    for i in range(len(data)):
        user_id = data[i]['data']['user_id']
        short_id = B64UUID(user_id[:32]).string + B64UUID(user_id[32:]).string
        data[i]['data']['user_id'] = short_id

        method = data[i]['data']['method']
        if method == 'GET':
            data[i]['data']['method'] = 1
        elif method == 'POST':
            data[i]['data']['method'] = 2
        elif method == 'PUT':
            data[i]['data']['method'] = 3
        else:
            data[i]['data']['method'] = 4

        url = data[i]['data']['url']
        if url == '/api/products/product/':
            data[i]['data']['url'] = 1
        else:
            data[i]['data']['url'] = 0

        indate = data[i]['data']['inDate']
        data[i]['data']['inDate'] = re.sub("[^0-9]","",indate[2:])

    return data