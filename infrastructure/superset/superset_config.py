import os

# ✅ Đổi từ airflow → superset
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/superset"

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "your-superset-secret-key")

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}