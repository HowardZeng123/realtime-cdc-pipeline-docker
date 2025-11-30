#!/bin/bash
set -e

# === BƯỚC 1: CHẠY VỚI 'root' ===
echo "Installing OS dependencies: librdkafka-dev and gcc..."
apt-get update
apt-get install -y -q librdkafka-dev gcc
echo "OS dependencies installed."

echo "Waiting for postgres at postgres:5432..."
while ! (exec 3<>/dev/tcp/postgres/5432) &>/dev/null; do
    echo "Postgres is unavailable - sleeping..."
    sleep 1
done
exec 3<&-
exec 3>&-
echo "Postgres is up and running!"


# === BƯỚC 2: CHUYỂN SANG USER 'airflow' ===
# SỬA LỖI Ở ĐÂY: Dùng 'su airflow' (bỏ dấu -) để giữ lại biến môi trường
exec su airflow << EOF
set -e

echo "Running pip install as 'airflow' user..."
if [ -f /opt/airflow/requirements.txt ]; then
    pip install -r /opt/airflow/requirements.txt
fi

echo "Running Airflow DB Upgrade as 'airflow' user..."
airflow db upgrade

echo "Creating admin user as 'airflow' user (if not exists)..."
if ! airflow users list | grep -q "admin"; then
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

echo "Starting Airflow $1 as 'airflow' user..."
exec airflow $1
EOF