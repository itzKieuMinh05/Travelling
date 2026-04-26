#!/bin/bash
# =============================================================
#  TRAVELLING — Reorganize Project Structure
#  Chạy từ root của repo: bash reorganize.sh
# =============================================================

set -e

echo "🚀 Bắt đầu reorganize project..."

# ─────────────────────────────────────────
# 1. TẠO CẤU TRÚC THƯ MỤC MỚI
# ─────────────────────────────────────────
echo "📁 Tạo thư mục mới..."

mkdir -p infrastructure/airflow/dags
mkdir -p infrastructure/gravitino
mkdir -p infrastructure/kafka
mkdir -p infrastructure/spark
mkdir -p infrastructure/superset
mkdir -p infrastructure/trino/catalog
mkdir -p infrastructure/minio

mkdir -p pipeline/ingestion
mkdir -p pipeline/processing
mkdir -p pipeline/transformation/dbt

mkdir -p scripts
mkdir -p data

# ─────────────────────────────────────────
# 2. DI CHUYỂN FILES
# ─────────────────────────────────────────
echo "📦 Di chuyển files..."

# --- Airflow DAGs ---
if [ -d "airflow/dags" ]; then
  cp -r airflow/dags/. infrastructure/airflow/dags/
  echo "  ✅ airflow/dags/ → infrastructure/airflow/dags/"
fi

# --- Gravitino config ---
if [ -d "gravitino" ]; then
  cp -r gravitino/. infrastructure/gravitino/
  echo "  ✅ gravitino/ → infrastructure/gravitino/"
fi

# --- Kafka config (nếu có config file) ---
if [ -d "kafka" ]; then
  # Script Python → pipeline/ingestion
  for f in kafka/*.py; do
    [ -f "$f" ] && cp "$f" pipeline/ingestion/ && echo "  ✅ $f → pipeline/ingestion/"
  done
  # Config files → infrastructure/kafka
  for f in kafka/*.yml kafka/*.yaml kafka/*.properties kafka/*.json; do
    [ -f "$f" ] && cp "$f" infrastructure/kafka/ && echo "  ✅ $f → infrastructure/kafka/"
  done
  # Dockerfile → infrastructure/kafka
  [ -f "kafka/Dockerfile" ] && cp kafka/Dockerfile infrastructure/kafka/ && echo "  ✅ kafka/Dockerfile → infrastructure/kafka/"
fi

# --- Spark ---
if [ -d "spark" ]; then
  # Python scripts → pipeline/processing
  for f in spark/*.py; do
    [ -f "$f" ] && cp "$f" pipeline/processing/ && echo "  ✅ $f → pipeline/processing/"
  done
  # Dockerfile + config → infrastructure/spark
  [ -f "spark/Dockerfile" ] && cp spark/Dockerfile infrastructure/spark/ && echo "  ✅ spark/Dockerfile → infrastructure/spark/"
  for f in spark/*.sh spark/*.conf spark/*.properties; do
    [ -f "$f" ] && cp "$f" infrastructure/spark/ && echo "  ✅ $f → infrastructure/spark/"
  done
fi

# --- dbt ---
if [ -d "dbt" ]; then
  cp -r dbt/. pipeline/transformation/dbt/
  echo "  ✅ dbt/ → pipeline/transformation/dbt/"
fi

# --- Superset ---
if [ -d "superset" ]; then
  cp -r superset/. infrastructure/superset/
  echo "  ✅ superset/ → infrastructure/superset/"
fi

# --- Trino ---
if [ -d "trino" ]; then
  cp -r trino/. infrastructure/trino/
  echo "  ✅ trino/ → infrastructure/trino/"
fi

# --- Scripts ---
if [ -d "scripts" ]; then
  # Chỉ copy nếu scripts/ ở root khác với target
  for f in scripts/*; do
    [ -f "$f" ] && cp "$f" scripts/ 2>/dev/null || true
  done
  echo "  ✅ scripts/ giữ nguyên"
fi

# ─────────────────────────────────────────
# 3. CẬP NHẬT docker-compose.yml
# ─────────────────────────────────────────
echo "🔧 Cập nhật docker-compose.yml..."

if [ -f "docker-compose.yml" ]; then
  cp docker-compose.yml docker-compose.yml.bak
  echo "  💾 Backup: docker-compose.yml.bak"

  # Thay thế các path cũ → path mới
  sed -i \
    -e 's|./airflow/dags|./infrastructure/airflow/dags|g' \
    -e 's|./airflow/logs|./infrastructure/airflow/logs|g' \
    -e 's|./airflow/plugins|./infrastructure/airflow/plugins|g' \
    -e 's|./gravitino|./infrastructure/gravitino|g' \
    -e 's|./superset|./infrastructure/superset|g' \
    -e 's|./trino/catalog|./infrastructure/trino/catalog|g' \
    -e 's|./trino/config.properties|./infrastructure/trino/config.properties|g' \
    -e 's|./trino/jvm.config|./infrastructure/trino/jvm.config|g' \
    -e 's|./trino/node.properties|./infrastructure/trino/node.properties|g' \
    -e 's|./spark|./infrastructure/spark|g' \
    -e 's|./kafka|./infrastructure/kafka|g' \
    docker-compose.yml

  echo "  ✅ docker-compose.yml đã được cập nhật"
fi

# ─────────────────────────────────────────
# 4. CẬP NHẬT .gitignore
# ─────────────────────────────────────────
echo "📝 Cập nhật .gitignore..."

cat >> .gitignore << 'EOF'

# Data files
data/
*.csv
*.zip
*.parquet

# Logs
infrastructure/airflow/logs/
infrastructure/airflow/plugins/

# Backup
docker-compose.yml.bak
EOF

echo "  ✅ .gitignore đã được cập nhật"

# ─────────────────────────────────────────
# 5. XÓA THƯ MỤC CŨ (sau khi đã copy xong)
# ─────────────────────────────────────────
echo ""
echo "⚠️  Kiểm tra lại trước khi xóa thư mục cũ:"
echo "   - infrastructure/ đã có đủ files chưa?"
echo "   - pipeline/ đã có đủ files chưa?"
echo ""
read -p "Xóa các thư mục gốc cũ? (y/N): " confirm

if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
  [ -d "airflow" ]  && rm -rf airflow  && echo "  🗑️  airflow/ đã xóa"
  [ -d "gravitino" ] && rm -rf gravitino && echo "  🗑️  gravitino/ đã xóa"
  [ -d "kafka" ]    && rm -rf kafka    && echo "  🗑️  kafka/ đã xóa"
  [ -d "spark" ]    && rm -rf spark    && echo "  🗑️  spark/ đã xóa"
  [ -d "dbt" ]      && rm -rf dbt      && echo "  🗑️  dbt/ đã xóa"
  [ -d "superset" ] && rm -rf superset && echo "  🗑️  superset/ đã xóa"
  [ -d "trino" ]    && rm -rf trino    && echo "  🗑️  trino/ đã xóa"
else
  echo "  ⏭️  Bỏ qua — thư mục cũ vẫn còn, tự xóa thủ công sau khi verify"
fi

# ─────────────────────────────────────────
# 6. IN KẾT QUẢ
# ─────────────────────────────────────────
echo ""
echo "✅ Hoàn thành! Cấu trúc mới:"
echo ""
find . -not -path './.git/*' \
       -not -path './data/*' \
       -not -path './docker-compose.yml.bak' \
       -not -name '*.pyc' \
  | sort \
  | sed 's|[^/]*/|  |g' \
  | head -60

echo ""
echo "📋 Bước tiếp theo:"
echo "  1. Kiểm tra docker-compose.yml — paths đã đúng chưa"
echo "  2. docker compose down -v"
echo "  3. docker compose up -d"