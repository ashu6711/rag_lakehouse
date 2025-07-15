from datetime import datetime, timedelta
from typing import List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.decorators.pyspark import pyspark_task
from pyspark.sql import SparkSession


from config.config import MIN_IO_ACCESS_KEY, MIN_IO_SECRET_KEY, MIN_IO_URL
from jobs.bronze_transformation import BronzeTransformation
from jobs.embedder import Embedding
from jobs.scraper import ScrapeBookDetails
from jobs.silver_transformation import SilverTransformation

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'books_scraping_pipeline',
    default_args=default_args,
    description='Scrape books.toscrape.com and process data',
    schedule_interval=None,  # Run daily
    catchup=False,
    tags=['scraping', 'books', 'etl'],
)

# Define tasks
scrape = PythonOperator(
    task_id='scrape',
    python_callable=ScrapeBookDetails().execute,
    dag=dag,
)

# Set task dependencies
@pyspark_task(conn_id='spark_default', dag=dag, config_kwargs={
        "spark.master": "spark://spark-master:7077",
        "spark.driver.memory": "2g"
    })
def bronze(scrape_list: List, spark:SparkSession, **context):

    a = BronzeTransformation(scrape_list=scrape_list).execute(spark=spark,context=context)
    pass

@pyspark_task(conn_id='spark_default', dag=dag, config_kwargs={
        "spark.master": "spark://spark-master:7077",
        "spark.driver.memory": "2g"
    })
def silver(scrape_list: List, spark:SparkSession, **context):

    a = SilverTransformation(scrape_list).execute(spark=spark,context=context)
    pass

@pyspark_task(conn_id='spark_default', dag=dag, config_kwargs={
        "spark.master": "spark://spark-master:7077",
        "spark.driver.memory": "2g"
    })
def embed(scrape_list: List, spark:SparkSession, **context):

    a = Embedding(scrape_list).execute(spark=spark,context=context)
    pass



scrape >> bronze(scrape_list=scrape.output) >> silver(scrape_list=scrape.output) >> embed(scrape_list=scrape.output)