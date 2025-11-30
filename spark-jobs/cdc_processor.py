import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, LongType

# Lấy biến môi trường (từ docker-compose)
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "broker:29092")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/cdc_data?ssl=false")

# === 1. ĐỊNH NGHĨA SCHEMA ===
# Đây là cấu trúc JSON mà Debezium publish lên Kafka.
# Chúng ta chỉ quan tâm đến 'payload' (dữ liệu) và 'op' (thao tác).

# Schema cho bảng 'customers'
CUSTOMERS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("address", StringType()),
    StructField("created_at", TimestampType()) # Không dùng nhưng phải khai báo
])

# Schema cho bảng 'products'
PRODUCTS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", DecimalType(10, 2)),
    StructField("created_at", TimestampType())
])

# Schema cho bảng 'orders'
ORDERS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("status", StringType()),
    StructField("total_amount", DecimalType(10, 2))
])

# Schema cho bảng 'order_items'
ORDER_ITEMS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType())
])

# Schema chung của Debezium (phần chúng ta quan tâm)
DEBEZIUM_SCHEMA = StructType([
    StructField("before", StringType()), # Dùng cho delete
    StructField("after", StringType()),  # Dùng cho create/update
    StructField("op", StringType()),     # 'c' (create), 'u' (update), 'd' (delete)
    StructField("ts_ms", LongType())   # Timestamp (phiên bản)
])

# === 2. HÀM HELPER ĐỂ XỬ LÝ STREAM ===
def process_stream(spark, topic_name, payload_schema, clickhouse_table):
    """
    Hàm này đọc 1 topic Kafka, xử lý và ghi vào 1 bảng ClickHouse
    """
    
    # 1. Đọc stream từ Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Parse JSON của Debezium
    # 'value' là JSON, chúng ta parse nó dùng DEBEZIUM_SCHEMA
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), DEBEZIUM_SCHEMA).alias("data")) \
        .select("data.*")

    # 3. Xử lý logic CDC (Insert, Update, Delete)
    # Nếu 'op' = 'd' (delete), data nằm trong 'before'.
    # Nếu 'op' = 'c' hoặc 'u' (create/update), data nằm trong 'after'.
    # Chúng ta cũng tạo cột 'sign' (1 cho insert/update, -1 cho delete)
    
    # Parse JSON lồng (payload_schema là schema của bảng, vd: CUSTOMERS_SCHEMA)
    transformed_df = parsed_df \
        .withColumn("payload", 
            when(col("op") == "d", col("before")) # Lấy data cũ nếu là delete
            .otherwise(col("after"))               # Lấy data mới nếu là create/update
        ) \
        .withColumn("payload", from_json(col("payload"), payload_schema)) \
        .withColumn("sign", 
            when(col("op") == "d", lit(-1)) # Dấu hiệu delete
            .otherwise(lit(1))              # Dấu hiệu insert/update
        ) \
        .select(
            "payload.*", # Lấy các cột (id, name, email...)
            "ts_ms",     # Cột phiên bản
            "sign"       # Cột dấu hiệu
        )

    # 4. Ghi stream vào ClickHouse (dùng forEachBatch)
    def write_to_clickhouse(batch_df, batch_id):
        print(f"Writing batch {batch_id} to ClickHouse table {clickhouse_table}...")
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", CLICKHOUSE_URL) \
                .option("dbtable", clickhouse_table) \
                .option("user", "admin") \
                .option("password", "admin") \
                .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
                .option("batchsize", "5000") \
                .option("isolationLevel", "NONE") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} written successfully.")
        except Exception as e:
            print(f"❌ Error writing batch {batch_id}: {e}")

    # 5. Bắt đầu query
    query = transformed_df \
        .writeStream \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", f"/tmp/checkpoints/{topic_name}") \
        .start()
        
    return query

# === 3. KHỞI TẠO SPARK VÀ CHẠY CÁC STREAM ===
def main():
    spark = SparkSession.builder \
        .appName("CDC_Kafka_to_ClickHouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN") # Giảm log nhiễu
    
    print("🚀 Spark Session created. Starting streams...")

    # Chạy 4 stream song song cho 4 bảng
    query_customers = process_stream(spark, "cdc.public.customers", CUSTOMERS_SCHEMA, "cdc_data.customers")
    query_products = process_stream(spark, "cdc.public.products", PRODUCTS_SCHEMA, "cdc_data.products")
    query_orders = process_stream(spark, "cdc.public.orders", ORDERS_SCHEMA, "cdc_data.orders")
    query_order_items = process_stream(spark, "cdc.public.order_items", ORDER_ITEMS_SCHEMA, "cdc_data.order_items")

    # Chờ tất cả các stream... (nếu 1 cái sập, tất cả sẽ dừng)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()