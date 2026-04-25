-- Tạo thêm database cho Superset (dùng chung postgres)
CREATE DATABASE superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO airflow;
