# ETL_Pipeline 프로젝트

- Extract, Transform, Load (데이터 수집, 정제, 저장)를 만들어보고<br>
이를 통해 Data Warehouse를 구축하는 인사이트를 얻기 위해 기획되었습니다. 

<br>

## 전체 프로젝트 구조

<img src='Work_SG\img\DF Service Guide.jpg' >

## 담당한 역할

- CP1
    - 기본적인 ETL 파이프라인 구축
    - AWS S3에 기본적인 Data Lake 구축

- CP2
    - ETL 파이프라인 구축
      - API 서비스에서 데이터를 추출(Extract), 변환(Transform), 압축(Compress), 적재(Load) <br>하기 위한 파이썬 모듈을 적용해본다.
      - AWS S3 버킷에 Raw 데이터를 적재하여 Data Lake를 구축한다.
      - Data Lake에서 데이터를 불러온 뒤, EDA를 거쳐 시각화를 하기위해<br> Data Warehouse에 정제된 데이터 적재

      <br>

    - 워크플로 자동화
      - Airflow를 구동하기 위한 리눅스 기반 환경 구축
      - API 데이터의 특성에 맞게 DAG 구축
      - Airflow 서버에서 DAG를 실행하여 워크플로 자동화 구현

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
- gzip 모듈을 사용한 데이터 압축 수행

> Load (적재)
> 
- boto3 모듈을 사용한 aws s3 서비스에 데이터 적재

<br>

## Airflow

> Workflow (워크플로)
> 
- apache-airflow 모듈을 사용해 DAG 생성
- Oracle Virtualbox 로 리눅스 환경 구축 후 워크플로 자동화
- Docker yml 파일로 개발 환경 구축 간소화

<br>

## AWS

> 인증
> 
- IAM 서비스를 사용해서 aws 접근 제어

> 데이터 적재
> 
- S3 서비스를 사용해서 aws 서비스에 추출, 변환한 데이터 적재

<br>

# 회고

## KTP 회고

> Keep (지속할 것) : 긍정적인 요소
> 
- MVP를 만들기 위해 빠르고 유연한 의사결정이 이루어진 점
- 각자 할당된 부분을 달성하기 위해 생소한 라이브러리를 배워야 했음에도
포기하지 않고 최선을 다한 점
- 각자의 역할이 명확하게 나뉘어 혼선을 빚지 않은 점
- 피드백을 받았을 때 빠르게 수용하고 개선해 나간 점

> Problem (해결할 것) : 부정적인 요소
> 
- 새롭게 적용한 라이브러리의 숙련도가 충분하지 않아 목표했던 만큼의
시너지가 일어나지 못한 점
- 여러가지 이유로 일정이 미뤄져 급하게 마무리 지어야 했던 점

> Try (시도할 것) : Problem에 대한 해결책, 잘 하고 있는 것을 더 잘하기 위해서는?
> 
- 롤 체인지를 하고 나서도 리뷰가 가능할 정도로 숙련도를 높이고 프로젝트에 참여한다.
- todo 리스트를 좀 더 철저하게 작성하고 일정 관리에 집중한다.

## 느낀점

- 개인적으로 포트폴리오를 작성했다면 하지 못했을 수준의 프로젝트에 참여하게 되어
아주 좋은 경험이 되었다.
- 정해진 일정 안에서 한 두가지가 지연되기 시작했을 때 어떻게 영향을 미치는지
잘 알게 되었고 일정 관리에 좀 더 신경써야겠다.
- 개인적으로 고도화를 진행하면서 좀 더 완성도 높은 결과물을 만들어내고 싶다.

<br>

---
### 개발 로그
[LOG.md](Work_SG/LOG.md)
