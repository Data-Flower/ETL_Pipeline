def SG_partitioning(data):
        import gzip
        import json

        if len(data['data']) != 0:
            year = data['data'][0]['감귤'][0]['11000101'][0]['ADJ_DT'][0:4]
            month = data['data'][0]['감귤'][0]['11000101'][0]['ADJ_DT'][4:6]
            date = data['data'][0]['감귤'][0]['11000101'][0]['ADJ_DT']

        directory = f'{year}/{month}/{date}.json.gz'
        compressed_data = gzip.compress(json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8'))

        return compressed_data, directory