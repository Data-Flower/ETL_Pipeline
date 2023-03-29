
print("this is modules/times_ __init__.py")

def timestamp_to_datetime(timestamp):
    """ 타임스탬프를 datetime으로 변환하는 함수 """
    import datetime

    return datetime.datetime.fromtimestamp(timestamp)

def datetime_to_timestamp(datetime):
    """ datetime을 타임스탬프로 변환하는 함수 """
    import time

    return time.mktime(datetime.timetuple())

def string_to_datetime(string):
    """ 문자열을 datetime으로 변환하는 함수 """
    import datetime

    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%fZ')

def string_to_timestamp(string):
    """ 문자열을 타임스탬프로 변환하는 함수 """

    return datetime_to_timestamp(string_to_datetime(string))


# [x] : SG 시간 데이터 변환
def SG_data_split(data):
    first_hour = data[0]['data']['inDate'][6:8]
    date_split = [0]

    for i in range(len(data)):
        next_hour = data[i]['data']['inDate'][6:8]
        if first_hour != next_hour:
            first_hour = next_hour
            date_split.append(i)
    date_split.append(100)

    result = []
    for i in range(len(date_split)-1):
        result.append(data[date_split[i]:date_split[i+1]])
    return result
