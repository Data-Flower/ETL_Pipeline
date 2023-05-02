def extract(url, params):
    '''
    api 데이터 호출 함수
    '''
    import requests
    import xmltodict

    response = requests.get(url,params=params)
    html = response.text
    html_dict = xmltodict.parse(html)
    return html_dict