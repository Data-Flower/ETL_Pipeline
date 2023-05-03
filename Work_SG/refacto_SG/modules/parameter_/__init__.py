def page_param(page, s_date, s_bubin, s_pummok):
    '''
    파라미터 설정 함수
    '''
    import os
    from dotenv import load_dotenv
    load_dotenv()

    garak_id = os.environ.get('garak_id')
    garak_passwd = os.environ.get('garak_passwd')

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
    return params

def url():
    url = 'http://www.garak.co.kr/publicdata/dataOpen.do?'
    return url

def lists():
    '''
    법인, 품목 리스트
    '''
    bubin_list = ['11000101','11000102','11000103','11000104','11000105','11000106']
    pummok_list = ['감귤','감자','건고추','고구마','단감','당근','딸기','마늘','무',
                    '미나리','바나나','배','배추','버섯','사과','상추','생고추','수박',
                    '시금치','양배추','양상추','양파','오이','참외','토마토','파',
                    '포도','피망','호박']
    return bubin_list, pummok_list