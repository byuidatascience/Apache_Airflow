from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pymongo import MongoClient

def fetch_latest_stock():
    mongo_uri = os.getenv('MONGODB_URI')
    client = MongoClient(mongo_uri)
    db = client.get_default_database()
    collection = db['daily_prices']
    latest = collection.find_one(sort=[('datetime', -1)])
    if latest:
        print(f"Latest stock entry: {latest}")
    else:
        print("No stock data found.")

with DAG(
    dag_id='example_mongo_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['example'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_latest_stock',
        python_callable=fetch_latest_stock
    )
