from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import shutil
from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.functions import col, regexp_extract, regexp_replace, when, lit, date_format
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, DateType


from config.config import BUCKET, MIN_IO_ACCESS_KEY, MIN_IO_SECRET_KEY, MIN_IO_URL
from helper.minio_manager import MinIOHelper


class SilverTransformation:

    def __init__(self, scrape_list):
        self.scrape_path = scrape_list[0]
        self.minio_bronze_file_loc = self.scrape_path.replace("raw","bronze")
        self.minio_silver_file_loc = self.scrape_path.replace("raw","silver")
        self.local_bronze_file_loc = f'shared_data/{self.scrape_path.replace("raw","bronze")}'
        self.local_silver_file_loc = f"shared_data/{self.minio_silver_file_loc}"

        self.silver_schema = {
            "run_id": ("run_id", StringType()),
            "run_date": ("run_date",StringType()),
            "title": ("title", StringType()),
            "price_clean": ("price", DoubleType()),
            "price_text": ("price_text", StringType()),
            "currency_symbol": ("currency_symbol", StringType()),
            "quantity": ("quantity",IntegerType()),
            "stock_status": ("stock_status",StringType()), 
            "description": ("description", StringType()),
            "category": ("category", StringType()),
            "review_count":("review_count", IntegerType()),
            "book_url": ("book_url",StringType()),
            "scraped_at": ("scraped_at", StringType()),
            "id": ("id", StringType())
        }
    
    def _parse_bronze_data(self, df):

        print(df.columns)

        #adding run_id in the  df
        df = df.withColumn("run_id", lit(self.scrape_path.split('/')[0]))

        df = df.withColumn("run_date", date_format("scraped_at", "yyyy-MM-dd"))

        #extracting currency symbol
        df = df.withColumn("currency_symbol", regexp_extract("price", r"^(\D+)", 1))

        #extracting clean price
        df = df.withColumn("price_clean", regexp_replace("price", r"^(\D+)", ""))

        df = df.withColumnRenamed('price', 'price_text')

        df = df.withColumn(
            "stock_status", 
            regexp_extract("availability", r"^([^\(]+)", 1)
        )

        # Extract quantity inside parentheses (digits)
        df = df.withColumn(
            "quantity", 
            regexp_extract("availability", r"\((\d+)", 1).cast("int")
        )

        # Handle 'Out of stock' or missing quantity (set quantity = 0)
        df = df.withColumn(
            "quantity", 
            when(col("quantity").isNull(), lit(0)).otherwise(col("quantity"))
        )

        print(df.columns)
        return df

    def _correct_dtypes(self, df):

        for i, j in self.silver_schema.items():
            df = df.withColumn(j[0], col(i).cast(j[1]))
        
        return df

        
    
    def execute(self,spark: SparkSession, **context):

        self.minio = MinIOHelper(endpoint=MIN_IO_URL, access_key=MIN_IO_ACCESS_KEY, secret_key= MIN_IO_SECRET_KEY)

        #fetch all the raw json files which are scraped
        self.scrape_list = self.minio.list_objects(bucket_name=BUCKET, prefix=self.minio_bronze_file_loc)

        os.makedirs(self.local_silver_file_loc, exist_ok=True)
        
        data_list = list()
        if self.scrape_list:
            
            max_threads = 10
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                files = {executor.submit(self.minio.download_file, BUCKET, file_name['name'], os.path.join(self.local_bronze_file_loc,file_name['name'].split('/')[-1])): file_name for file_name in self.scrape_list}

                for future in as_completed(files):
                    url = files[future]
                    try:
                        data = future.result()
                    except Exception as e:
                        print(f"Failed to Fetch Data")

            
            df = spark.read.parquet(self.local_bronze_file_loc)

            df_parsed = self._parse_bronze_data(df)        
            df = self._correct_dtypes(df_parsed)

            

            df = df.select(*list(self.silver_schema.keys()))

            print(df.show(5))
            df.write.mode("overwrite").parquet(self.local_silver_file_loc)

            if os.listdir(self.local_silver_file_loc):
                for i in os.listdir(self.local_silver_file_loc):
                    if i.endswith('.parquet'):
                        print(i)
                        self.minio.upload_file(bucket_name=BUCKET, object_name=f"{self.minio_silver_file_loc}/{i}", file_path=os.path.join(self.local_silver_file_loc,i))

                shutil.rmtree(f'shared_data/{self.scrape_path.split('/')[0]}')
                print(f"File Upload to min io. Loc: {self.minio_silver_file_loc}")
                return True
        
        else:
            print(f"No bronze file found")
            raise "No! Bronze file found"
