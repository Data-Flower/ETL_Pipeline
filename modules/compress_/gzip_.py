
def compress_test():
    print('gzip compress test')

def decompress_test():
    print('gzip decompress test')

def compress(data, compress_level=9):
    """ 데이터를 압축하는 함수 """
    if type(data) == str:
        data = data.encode()
        
    import gzip
    return gzip.compress(data, compress_level)

def decompress(data):
    """ 데이터를 압축해제하는 함수 """
    import gzip
    return gzip.decompress(data)