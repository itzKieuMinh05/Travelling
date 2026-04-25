import os

# Kết nối database cho Superset metadata
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/superset"

# Secret key
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "your-superset-secret-key")

# Cho phép iframe embedding
WTF_CSRF_ENABLED = True
SESSION_COOKIE_SAMESITE = "Lax"

# Feature flags hữu ích
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# Cache (simple in-memory cho dev)
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
