from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'hao_data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_revenue_report',
    default_args=default_args,
    description='Trigger Spark Batch Job from Airflow',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily', # Chạy mỗi ngày 1 lần
    catchup=False,
) as dag:

    # Task 1: In thông báo
    start_task = BashOperator(
        task_id='start_process',
        bash_command='echo "Starting Batch Job..."'
    )

    # Task 2: Trigger Spark Job (Dùng docker exec)
    # Chúng ta ra lệnh cho container "spark-master" chạy lệnh spark-submit
    # Lưu ý: File python phải nằm trong thư mục /opt/spark-jobs/ (đã mount vào spark-master)
    run_spark_job = BashOperator(
        task_id='submit_spark_job',
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark-jobs/daily_report.py
        """
    )

    # Task 3: Thông báo xong
    end_task = BashOperator(
        task_id='end_process',
        bash_command='echo "Job Finished Successfully!"'
    )

    # Luồng chạy
    start_task >> run_spark_job >> end_task