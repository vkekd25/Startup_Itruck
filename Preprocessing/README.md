**전처리**

registration_preprocessing.ipynb - 영업용현황 전처리파일  
new_preprocessing.ipynb - 신규등록 전처리파일  
tranfer_preprocessing.ipynb - 이전등록 전처리파일  
change_preprocessing.ipynb - 구조변경 전처리파일  
cancel_preprocessing.ipynb - 말소등록 전처리파일  
pledge_preprocessing.ipynb - 저당등록 전처리파일

resgistration.parquet - 전처리된 영업용현황 데이터  
new_completed.parquet - 전처리된 신규등록 데이터  
transfer_completed.parquet - 전처리된 이전등록 데이터  
change_completed.parquet - 전처리된 구조변경등록 데이터  
cancel_completed.parquet - 전처리된 말소등록 데이터
pledge_preprocessing.ipynb - 전처리된 저당등록 데이터


**영업용화물차량 등록현황 전처리 과정**  
1. 중복 칼럼 확인 (칼럼별로 쉼표,띄워쓰기,오카로 같은 칼럼이 다른 칼럼으로 나뉘어 기록된 경우가 있었음)  
2. 중복 칼럼 하나의 칼럼으로 합침  
3. 컬럼별 결측치 확인  

**구조변경 전처리**  
1. pickling 복호화  
2. 컬럼명 변경  
3. CONCAT으로 합침  
4. 데이터 타입 변경  
5. 불필요한 컬럼 DROP  
6. 결측치 대체 및 제거  
7. 데이터 분류를 위한 추가컬럼 생성  
8. parquet 파일 경량화  

**저당데이터 전처리 과정**  
1. 분석에 필요하지 않은 칼럼 제거  
2. 빅쿼리 적제를 위해 칼럼명 모두 영문으로 변경  
3. 칼럼별 결측치 확인  
4. amount의 경우 int/float으로 변환  
5. date 칼럼 datetime으로 자료형 변환  
6. vehicle cat,transmission의 경우 찾아서 채움  
7. max_load의 null값에서 사다리차의 경우 null로 남겨둠

