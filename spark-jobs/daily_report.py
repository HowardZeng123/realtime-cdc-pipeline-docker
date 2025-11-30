from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Daily_Report_Job") \
        .getOrCreate()

    # Ví dụ: Tạo một báo cáo đơn giản
    print("============================================")
    print("🚀 STARTING DAILY REPORT...")
    print("============================================")
    
    # Giả lập xử lý data
    data = [("Order_1", 100), ("Order_2", 200), ("Order_3", 300)]
    df = spark.createDataFrame(data, ["Order", "Amount"])
    
    total = df.groupBy().sum("Amount").collect()[0][0]
    
    print(f"💰 TOTAL REVENUE CALCULATED: ${total}")
    print("============================================")
    print("✅ REPORT COMPLETED")
    
    spark.stop()

if __name__ == "__main__":
    main()