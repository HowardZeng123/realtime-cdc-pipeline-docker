#!/bin/bash
set -e

echo "--- [1/3] Installing dependencies ---"
# Cài thư viện Python
if [ -f /opt/requirements.txt ]; then
    pip install -r /opt/requirements.txt
fi

# Cài kafkacat và jq để check topic
apt-get update -q && apt-get install -y -q kafkacat jq

echo "--- [2/3] Waiting for Kafka Topic (cdc.public.orders) ---"
echo "🚀 Spark Job đang chờ bro chạy lệnh CURL để Debezium tạo topic..."

# Vòng lặp check topic (Check tối đa 30 lần, mỗi lần 5s = 150s)
counter=0
max_retries=30

# Lưu ý: Trong file .sh này dùng $ thường, không dùng $$
until kafkacat -b broker:29092 -L -J | jq -e '.topics[] | select(.topic == "cdc.public.orders")' > /dev/null 2>&1; do
  if [ $counter -eq $max_retries ]; then
    echo "❌ Timeout: Topic 'cdc.public.orders' chưa được tạo sau nhiều lần thử."
    echo "⚠️  Bro đã chạy lệnh CURL chưa?"
    exit 1
  fi
  
  echo "⏳ [$counter/$max_retries] Topic chưa thấy... Đợi 5s..."
  sleep 5
  counter=$((counter+1))
done

echo "✅ Topic 'cdc.public.orders' đã tồn tại! Let's go!"

echo "--- [3/3] Starting Spark Submit ---"
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.github.housepower:clickhouse-native-jdbc-shaded:2.7.1 \
  --conf spark.sql.adaptive.enabled=true \
  /opt/spark-jobs/cdc_processor.py