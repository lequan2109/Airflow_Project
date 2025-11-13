from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/scripts')
from extract_stock_data import download_stock_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='stock_etl_dag',
    default_args=default_args,
    description='ETL Pipeline: Extract -> Transform -> Load Stock Data',
    schedule_interval='0 7 * * *',  # chạy mỗi 7h sáng hằng ngày
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=['ETL', 'Spark', 'Stocks'],
    template_searchpath=['/opt/airflow/sql']
    
) as dag:

    # --- Extract ---
    extract_task = PythonOperator(
        task_id='extract_stock_data',
        python_callable=download_stock_data,
    )

    # --- Transform ---
    transform_task = SparkSubmitOperator(
        task_id='transform_stock_data',
        application='/opt/airflow/spark_jobs/transform_stock_data_v3.py',
        conn_id='spark_default',
        
        conf={'spark.master': 'local[*]'},
        executor_memory='2g',
        verbose=True,
    )

    # --- Create Table ---
    create_table = PostgresOperator(
        task_id='create_stock_table',
        postgres_conn_id='postgres_default', # <-- Nhớ tạo Connection Id này trong Airflow UI
        sql='create_stock_table.sql',
    )

    # --- Load Data ---
    from airflow.operators.python import PythonOperator
    import psycopg2

    # ===== SỬA LỖI: Cập nhật hàm load_data_to_postgres =====
    def load_data_to_postgres():
        import csv
        import psycopg2

        # 1. Danh sách cột này PHẢI KHỚP với thứ tự trong file CSV
        # (tức là schema của Spark UDF trong file transform_stock_data_v3.py)
        csv_columns = (
            'date', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'industry', 
            'daily_return', 'price_range', 'volume_sma_5', 'sma_5', 'sma_20', 
            'ema_12', 'ema_26', 'macd', 'rsi_14'
        )
        
        # 2. Danh sách cột này PHẢI KHỚP với tên cột trong bảng SQL
        # (trong file create_stock_table.sql)
        # Lưu ý: Thứ tự ở đây không nhất thiết phải giống csv_columns, 
        # nhưng để đơn giản, chúng ta giữ chúng giống hệt nhau.
        table_columns = (
            'date', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'industry', 
            'daily_return', 'price_range', 'volume_sma_5', 'sma_5', 'sma_20', 
            'ema_12', 'ema_26', 'macd', 'rsi_14'
        )

        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE stock_prices_all;")
        with open('/opt/airflow/data/output/cleaned_stock.csv', 'r') as f:
            next(f)  # bỏ header
            
            # 3. Chỉ định rõ các cột khi COPY
            # Điều này an toàn hơn nhiều so với việc không chỉ định
            cur.copy_from(
                f, 
                'stock_prices_all', 
                sep=',', 
                columns=table_columns # <-- Chỉ định cột của bảng DB
            )
            
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Dữ liệu đã được load vào bảng stock_prices_all thành công!")
    # ========================================================

    load_data = PythonOperator(
        task_id='load_stock_data',
        python_callable=load_data_to_postgres,
    )

    # Định nghĩa dependencies (đã xóa dòng lặp lại)
    extract_task >> transform_task >> create_table >> load_data