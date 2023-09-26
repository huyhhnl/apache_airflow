## 1. Cài đặt airflow bằng docker
## 2. Cài đặt airflow, mongodb, spark bằng máy ảo ubuntu cho project
## 3. Tạo datapipeline với Airflow
### 3.1 DAG là gì ?
### 3. 2 Operator là gì ?
- Operator được sử dụng để định nghĩa 1 Task trong DAG, mỗi Operator chỉ nên định nghĩa 1 Task duy nhất. Có 3 loại Operator :
  - Action Operator
  - Tranfer Operator
  - Sensor Operator
### 3.3 Provider là gì ?
- Dùng để tương tác với các công cụ khác nhau như spark, aws ..., nên airflow cần có những provider
### 3.4 Lập lịch cho DAG
### 3.5 Backfilling và catchup
## 4. Chạy datapipeline song song
- Cần thay đổi Executor của airflow từ SequentialExecutor sang LocalExecutor
- Thay đổi database engine mặc định của airflow từ sqllite sang postgresql
## 5. 1 số khái niệm khác
### 5.1 SubDAGs và Tasks Group
### 5.2 Xcoms
### 5.3 Kích hoạt Task theo điều kiện
