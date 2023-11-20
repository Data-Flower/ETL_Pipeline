# 1주차 개발로그 (3/27 ~ 4/2)

<details>
<summary> 3/27 업데이트 </summary>

- cp1 프로젝트 코드 업로드
</details>

<br>

<details>
<summary> 3/28 업데이트 </summary>

- 데이터 선정
- cp2 api test 코드 업로드
</details>

<br>

<details>
<summary> 4/2 업데이트 </summary>

> EDA 진행
- 대상 : 데이터셋 중 201601 파일
- 내용
    - CMP_NM : 법인. 중요하지 않은 특성이므로 제거
    - DAN_NM : 단위. kg이 거의 대부분이므로 나머지 단위 제거
    - POJ_NM : 포장. 상자와 결측치가 대부분이므로 나머지 제거        
    - SIZE_NM : 크기. 결측치가 대부분이므로 삭제
    - LV_NM : 등급. 특 등급이 대부분이므로 특, 상, 보통, 등외 4가지로 정리
    - DANQ : 단위중량. 이상치와 취소된 거래 제거
    - QTY : 물량. 마이너스(-) 의 경우 취소된 거래이므로 제거
    - COST : 단가. 취소된 거래 제거
    - TOT_QTY : 총 물량. DANQ * QTY로 계산 가능
    - TOT_AMT : 총 금액. QTY * COST로 계산 가능
- 결과
    - csv 파일 : EDA 전 327mb -> EDA 후 213mb
    - json 파일 : EDA 전 1.4gb -> EDA 후 617mb

> 파일 압축 테스트
- json 원본 파일을 gzip, bz2, lzma 라이브러리로 각각 압축 후 압축률 비교
- json 원본 파일 : 617mb
  - gzip : 21.5mb, 96.62%
  - bz2 : 14.3mb, 97.69%
  - lzma : 18.1mb, 97.07%

> 발생한 오류와 해결
- pandas 라이브러리 사용 중 SettingWithCopyWarning 에러 발생 <br>
  코드가 작동은 했지만 지속적으로 에러 메시지 발생<br>
    - warning 라이브러리를 사용해서 해결
```python
import warnings
warnings.filterwarnings('ignore')
```
- EDA 진행 후 저장한 json 파일을 열 때 <br>
  invalid string length 에러 발생
    - orient 옵션 'table' -> 'index' 변경 후 해결
```python
convert_data.to_json('CP1/eda.json', orient='index', indent=4)
```

- EDA 진행 후 저장한 json 파일을 열 때 <br>
  데이터가 unicode 형태로 한글이 저장되는 현상 발생
    - pandas의 to_json 인자값 중 force_ascii=False 사용으로 해결
```python
convert_data.to_json('CP1/eda.json', orient='index', indent=4, force_ascii=False)
```

- json 데이터 파싱에러 발생 <br>
  json.decoder.JSONDecodeError: Expecting value: line 12 column 1 (char 22)
    - xmltodict 라이브러리 사용으로 해결
```python
import xmltodict
page = requests.get(url)
html = page.text

html_dict = xmltodict.parse(html)
```
</details>

<br>

# 2주차 개발로그 (4/3 ~ 4/9)

<details>
<summary> 4/6 업데이트 </summary>

- 가락시장 API를 이용한 ETL Pipeline 코드 업로드
- AWS S3에 적재된 데이터를 불러온 뒤 json으로 반환하는 코드 업로드
</details>

<br>

<details>
<summary> 4/9 업데이트 </summary>

> Airflow 구동을 위한 아마존 EC2 인스턴스 생성
>
- ubuntu 20.04 LTS (HVM), SSD Volume Type
- ssh 접속 오류 발생

<img src='img\20230406_231747.png'>

- 확인 결과 sk 브로드밴드 사용시 22번 포트 사용 불가로 ssh 접속 안되는 현상 발생
- EC2 인스턴스 연결로 접속
- t2.micro 프리티어 (1vCPU, 1GiB 메모리) 로는 Airflow 구동 시 다운되는 문제 발생
    - 로컬에서 진행하기로 결정

> 윈도우10 로컬에 WSL로 리눅스 환경구축
>
- Airflow는 리눅스에서만 구동되기 때문에 로컬로 진행하려면
리눅스 기반 환경이 필요하다.
- 마이크로소프트 스토어에서 ubuntu 20.04 설치

<img src='img\20230411_210135.png'>

- systemd 오류 발생

<img src='img\20230411_214044.png'>

    - 윈도우 기반의 WSL에서 발생하는 오류로, 정식 Linux OS가 아니라서 발생한다.<br>
    Linux의 initd 프로세스가 근래에 systemd로 대체되었지만 WSL은 여전히
    initd 프로세스(PID 1)가<br> 그 역할을 맡고 있어 호환성에 문제가 있다.
    - 진행 불가로 판단하고 가상화 머신을 이용하기로 결정.

> Oracle VM VirtualBox로 가상화 머신 구동
>
- oracle virtualbox를 이용하여 가상화 구동 후 ubuntu 20.04 설치  
- mysql 8.0 + airflow 2.5.3 설치
- ssh를 이용할 수 없으므로 dag 파일을 전송할 다른 방법이 필요
    - virtualbox 에서 제공하는 공유폴더 기능을 이용하여 virtualbox_share 폴더 지정

    <img src='img\20230411_233951.png'>

    - mount -t vboxsf virtualbox_share /home/ente/virtualbox_share/ 명령어로
    ubuntu 상에서 동기화 후 공유폴더 이용 가능

    <img src='img\20230411_234517.png'>

- airflow standalone 명령어로 webserver와 scheduler 구동

<img src='img\VirtualBox_airflow_11_04_2023_23_50_34.png'>

- localhost:8080 으로 접속하여 airflow 정상 작동 확인

<img src='img\VirtualBox_airflow_11_04_2023_23_52_16.png'>

</details>

<br>

# 3주차 개발로그 (4/10 ~ 4/16)

<details>
<summary> 4/11 업데이트 </summary>

> Airflow DAG 생성
>
- 기존 ETL Pipeline 코드에서 함수단위로 Task 지정 후 T1 >> T2 >> T3 <br>로 DAG를 구성하였지만 구동 실패
    - Airflow의 Task는 독립적으로 실행되기 때문에 기본적으로 서로 통신할 수단이 없다.
    - 이를 해결하기 위해 XCom을 이용해 데이터 전송 가능.
    - 앞 함수의 return 값은 자동으로 xcom에 저장되므로 뒤 함수의 파라미터를 **context로 받은 뒤<br>xcom_pull을 이용하여 변수에 저장

    ```python
    from airflow.operators.python import PythonOperator
    from airflow.models import TaskInstance

    def extract():
        ...
        return html_dict

    def transform(**context):
        data = context['task_instance'].xcom_pull(task_ids='extract')
        ...
        return dictionary

    def load(**context):
        data = context['task_instance'].xcom_pull(task_ids='transform')
        ...
    ```
- XCom을 사용한 DAG 구동 실패
    - xcom은 task간의 통신을 위한 메모 정도의 목적으로 설계되었기 때문에 대용량 파일 전송 등의 용도로는 적합하지 않다.
    - MAX XCom size is 48kb
- Task를 하나로 통합하여 하나의 함수 안에서 ETL이 모두 이루어지도록 변경. DAG 구동 성공 
</details>

<br>

<details>
<summary> 4/13 업데이트 </summary>

- requirements 작성
- 병렬처리를 위한 ray test 코드 업로드
</details>