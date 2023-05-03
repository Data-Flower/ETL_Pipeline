from modules import load_, transform_

def etl_pipeline(date):
    data = transform_.transform(date)
    load_.load(data)
    # load_.save_local(date, data)

etl_pipeline('20230426')