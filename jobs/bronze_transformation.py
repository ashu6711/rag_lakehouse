from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import shutil
from pyspark.sql import SparkSession
from datetime import date

from config.config import BUCKET, MIN_IO_ACCESS_KEY, MIN_IO_SECRET_KEY, MIN_IO_URL
from helper.minio_manager import MinIOHelper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BronzeTransformation:

    def __init__(self, scrape_list):
        self.scrape_list = scrape_list[0]
        self.local_bronze_file_loc = f"shared_data/{self.scrape_list.replace("raw", "bronze")}"
        self.minio_bronze_file_loc =  self.scrape_list.replace("raw","bronze")

        self.bronze_schema = {
            "title_xpath": "title",
            "price_xpath": "price",
            "availability_xpaths": "availability",
            "description_xpaths": "description",
            "category_xpath": "category",
            "review_count_xpath": "review_count",
            "book_url": "book_url",
            "scrape_at": "scraped_at"
        }
    
    def execute(self,spark: SparkSession, **context):
 
        self.minio = MinIOHelper(endpoint=MIN_IO_URL, access_key=MIN_IO_ACCESS_KEY, secret_key= MIN_IO_SECRET_KEY)

        #fetch all the raw json files which are scraped
        self.scrape_list = self.minio.list_objects(bucket_name=BUCKET, prefix=self.scrape_list)
        
        data_list = list()
        if self.scrape_list:
            
            max_threads = 10
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                files = {executor.submit(self.minio.get_object_data, BUCKET, file_name['name']): file_name for file_name in self.scrape_list}

                for future in as_completed(files):
                    url = files[future]
                    try:
                        data = future.result()
                        data_list.append(json.loads(data))
                    except Exception as e:
                        print(f"Failed to Fetch Data")
        
            os.makedirs(self.local_bronze_file_loc, exist_ok=True)

            df = spark.createDataFrame(data_list)

            row_count = df.count()
            print(f"Number of rows to be written: {row_count}")

            # 2. If there are rows, show a sample.
            if row_count > 0:
                print("Showing a sample of the data:")
                df.show(10, truncate=False)
            else:
                # This is the most likely scenario
                print("DataFrame is empty. No Parquet files will be generated.")
            
            for old_name, new_name in self.bronze_schema.items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)

            df.write.mode("overwrite").parquet(self.local_bronze_file_loc)
            print(os.listdir(self.local_bronze_file_loc))

            if os.listdir(self.local_bronze_file_loc):
                for i in os.listdir(self.local_bronze_file_loc):
                    if i.endswith('.parquet'):
                        print(i)
                        self.minio.upload_file(bucket_name=BUCKET, object_name=f"{self.minio_bronze_file_loc}/{i}", file_path=os.path.join(self.local_bronze_file_loc,i))

                shutil.rmtree(self.local_bronze_file_loc)
                print(f"File Upload to min io. Loc: {self.minio_bronze_file_loc}")
                return True
        
        else:
            print(f"No scraped output file found")
            raise "No! Scraped file found"
    