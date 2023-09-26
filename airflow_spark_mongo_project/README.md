## Thiết lập DataPipeline cho dữ liệu lớn từ Cloud
### 1.Tổng quan
- Trong project này, ta sẽ thực hành để tạo một Data Pipeline hoàn chỉnh để có thể làm các thao tác như tải tập dữ liệu, import tập dữ liệu vào Database và xử lý dữ liệu với Spark. Sản phẩm sẽ là một Data Pipeline có các bước như sau:
![dag](images/dag.png)
### 2. Yêu cầu
- Cài airflow, spark, mongo trên máy ảo ubuntu
### 3. Tài nguyên
- [Answers.csv](https://drive.google.com/file/d/1gEU9KojKI3yMybjL0_tYsQnzRlUoBYro/view?usp=sharing)
File csv chứa các thông tin liên quan đến câu hỏi của hệ thống, với cấu trúc như sau:

  - Id: Id của câu trả lời.
  
  - OwnerUserId: Id của người tạo câu trả lời đó. (Nếu giá trị là NA thì tức là không có giá trị này).
  
  - CreationDate: Ngày câu hỏi được tạo.
  
  - ClosedDate: Ngày câu hỏi kết thúc (Nếu giá trị là NA thì tức là không có giá trị này).
  
  - Score: Điểm số mà người tạo nhận được từ câu hỏi này.
  
  - Title: Tiêu đề của câu hỏi.
  
  - Body: Nội dung câu hỏi.
- [Questions.csv](https://drive.google.com/file/d/1yH2lTY3mGOoGEDW1Y2lhzRwDuaZvI1KI/view?usp=sharing)
File csv chứa các thông tin liên quan đến câu trả lời và có cấu trúc như sau:

  - Id: Id của câu hỏi.
  
  - OwnerUserId: Id của người tạo câu hỏi đó. (Nếu giá trị là NA thì tức là không có giá trị này)
  
  - CreationDate: Ngày câu trả lờiđược tạo.
  
  - ParentId: ID của câu hỏi mà có câu trả lời này.
  
  - Score: Điểm số mà người trả lờinhận được từ câu trả lời này.
  
  - Body: Nội dung câu trả lời.
### 4. Tiến hành
- 1.  Task: start và end

Ta cần tạo 2 task start và end là các DummyOperator để thể hiện cho việc bắt đầu và kết thúc của DAG.

- 2. Task: branching

  - Tạo 1 task để kiểm tra xem hai file Questions.csv và Answers.csv đã được tải xuống để sẵn sàng import hay chưa.
  
  - Nếu file chưa được tải xuống thì bắt đầu quá trình xử lý dữ liệu (Chuyển đến task clear_file).
  - Nếu file đã được tải xuống thì sẽ kết thúc Pipleine (Chuyển đến task end).

- 3. Task: clear_file

  - Đây sẽ là Task đầu tiên trong quá trình xử lý dữ liệu, trước khi Download các file Questions.csv và Answers.csv thì bạn sẽ cần xóa các file đang tồn tại để tránh các lỗi liên quan đến việc ghi đè.

- 4. Task: dowload_question_file_task và dowload_answer_file_task
  - Tạo task để download file từ google driver
- 5. Task: import_questions_mongo và import_answers_mongo

  - Import các dữ liệu đó vào MongoDB.
- 6. Task: spark_process
     - dùng spark để tính toán xem mỗi câu hỏi đang có bao nhiêu câu trả lời.
- 7. Task: import_output_mongo

  - Sau khi đã xử lý xong dữ liệu, ta sẽ lưu các kết quả vào MongoDB từ file .csv đã được export từ Spark.
