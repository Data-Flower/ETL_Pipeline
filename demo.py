
# 1. 프로젝트에 사용할 데이터를 dict 형태로 정의한다.
env_test = {
    "env" : "test",
    "url" : "localhost:8080",
    "aws" : {
        "aws_access_key_id":"aws_key",
        "aws_secret_access_key":"aws_secret",
        "aws_s3_bucket_name":"my_bucket"
    }
}

"""
2. 프로젝트 아키텍쳐에서 
부모 클래스(Core)를 상속한 서브 프로젝트의 클래스(ETL_CP1)를 임포트한다.
"""
from Project.CP1.ETL_CP1 import ETL_CP1

# 3. 임포트한 자식 클래스를 생성(인스턴스화)한다.
# ★ 이때, 1에서 작성했던, 인스턴스 내부의 메서드가 동작하기 위해 필요한 데이터를 전달한다.
etl = ETL_CP1(env_test)

# 4. 생성한 인스턴스의 run 메서드를 호출한다.
# ★ 인스턴스의 외부에서 접근하는 메서드는 run 메서드로 한정한다.
# run 메서드가 필요한 데이터는 작성하는 메서드마다 다르겠지만
# 기본적으로 스케줄링 시간 간격(interval_minutes)을 전달한다.
etl.run(interval_minutes=1)

