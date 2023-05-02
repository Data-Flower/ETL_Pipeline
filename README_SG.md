# ETL_Pipeline 프로젝트

- Extract, Transform, Load (데이터 수집, 정제, 저장)를 만들어보고<br>
이를 통해 Data Warehouse를 구축하는 인사이트를 얻기 위해 기획되었습니다. 

<br>

# 프로젝트 목표

- CP1
    - 기본적인 ETL 파이프라인 구축
    - AWS S3에 기본적인 Data Lake 구축

- CP2
    - ETL 파이프라인 구축
      - API 서비스에서 데이터를 추출(Extract), 변환(Transform), 압축(Compress), 적재(Load) <br>하기 위한 파이썬 모듈을 적용해본다.
      - AWS S3 버킷에 Raw 데이터를 적재하여 Data Lake를 구축한다.
      - Data Lake에서 데이터를 불러온 뒤, EDA를 거쳐 시각화를 하기위해<br> Data Warehouse에 정제된 데이터 적재

      <br>

    - 자동화 스케쥴링
      - Airflow를 구동하기 위한 리눅스 기반 환경 구축
      - API 데이터의 특성에 맞게 DAG 구축
      - Airflow 서버에서 DAG를 실행하여 자동화 스케쥴링 구현

<br>

# Tech Stack

## Python

> Extract (추출)
> 
- requests 모듈을 사용한 데이터 받아오기
- dotenv 모듈을 사용해 환경변수 .env로 관리
- xmltodict 모듈을 사용해 html을 dict로 파싱

> Transform (변환)
> 

- gzip 또는 zlib 모듈을 사용한 데이터 압축 수행

> Load (적재)
> 
- boto3 모듈을 사용한 aws s3 서비스에 데이터 적재

> Scheduling (스케쥴링)
> 
- apache-airflow 모듈을 사용해 DAG 생성
- Oracle Virtualbox 로 리눅스 환경 구축 후 스케쥴링

<br>

## AWS

> 인증
> 
- IAM 서비스를 사용해서 aws 접근 제어

> 데이터 적재
> 
- S3 서비스를 사용해서 aws 서비스에 추출, 변환한 데이터 적재

<br>

---
### 개발 로그
[LOG.md](Work_SG/LOG.md)
