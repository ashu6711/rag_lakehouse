from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import pandas as pd
import chromadb
import os
from chromadb.utils import embedding_functions
from pyspark.sql import SparkSession, DataFrame
from langchain_experimental.text_splitter import SemanticChunker
from langchain_community.embeddings import HuggingFaceEmbeddings

from config.config import BUCKET, DB_COLLECTION_NAME, MIN_IO_ACCESS_KEY, MIN_IO_SECRET_KEY, MIN_IO_URL, CHROMA_HOST, CHROMA_PORT, EMBEDDING_MODEL
from helper.minio_manager import MinIOHelper

class Embedding:

    def __init__(self, scrape_list):
        self.scrape_path = scrape_list[0]
        self.minio_silver_file_loc = self.scrape_path.replace("raw","silver")
        self.local_silver_file_loc = f"shared_data/{self.minio_silver_file_loc}"
        self.collection_name = DB_COLLECTION_NAME

    def ingest_data_with_semantic_chunking(
        self,
        spark_df: DataFrame, 
        collection_name: str, 
        chroma_host: str, 
        chroma_port: int, 
        embedding_model_name: str
    ):

        print(f"Loading embedding model '{embedding_model_name}' for chunking...")
        model_kwargs = {'device': 'cpu'} # Use 'cuda' if you have a GPU
        encode_kwargs = {'normalize_embeddings': False}
        embedding_model = HuggingFaceEmbeddings(
            model_name=embedding_model_name,
            model_kwargs=model_kwargs,
            encode_kwargs=encode_kwargs
        )
        
        # Initialize the SemanticChunker with the compatible embedding model
        text_splitter = SemanticChunker(embedding_model)

        client = chromadb.HttpClient(host=chroma_host, port=chroma_port)
        
        chroma_ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_model_name)
        
        collection = client.get_or_create_collection(
            name=collection_name,
            embedding_function=chroma_ef
        )
        print(f"Successfully connected to collection '{collection_name}'.")
        print(spark_df.show(5))
        print("Collecting Spark DataFrame to driver for processing...")
        records = spark_df.collect()
        print(f"Processing {len(records)} records and performing semantic chunking...")

        all_chunks = []
        all_metadatas = []
        all_ids = []

        for row in records:

            description = f"Title: {row['title']}. Category: {row['category']}. Description: {row['description']}"
            chunks = text_splitter.create_documents([description])
            
            # For each chunk, create a record to be added to ChromaDB
            for i, chunk in enumerate(chunks):
                chunk_text = chunk.page_content


                
                # The metadata for each chunk should link back to the original book
                chunk_metadata = {
                    "original_book_id": str(row['id']),
                    "book_title": str(row['title']),
                    "book_source_url": str(row['book_url']),
                    "chunk_number": i + 1,
                    "category": str(row['category']),
                    "price": float(row['price_clean']) 
                }
                
                # The ID for each chunk must be unique
                chunk_id = f"book_{row['id']}_chunk_{i}"
                
                all_chunks.append(chunk_text)
                all_metadatas.append(chunk_metadata)
                all_ids.append(chunk_id)


        print(f"Generated a total of {len(all_chunks)} chunks. Ingesting into ChromaDB...")
        batch_size = 100
        for i in range(0, len(all_chunks), batch_size):
            batch_end = i + batch_size
            print(f"Processing batch {i//batch_size + 1}: chunks {i+1} to {min(batch_end, len(all_chunks))}")
            
            collection.add(
                ids=all_ids[i:batch_end],
                documents=all_chunks[i:batch_end],
                metadatas=all_metadatas[i:batch_end]
            )

        print("\nData ingestion with semantic chunking complete!")
        print(f"Total items in collection '{collection_name}': {collection.count()}")

    def execute(self,spark: SparkSession, **context):

        self.minio = MinIOHelper(endpoint=MIN_IO_URL, access_key=MIN_IO_ACCESS_KEY, secret_key= MIN_IO_SECRET_KEY)

        #fetch all the raw json files which are scraped
        self.scrape_list = self.minio.list_objects(bucket_name=BUCKET, prefix=self.minio_silver_file_loc)

        os.makedirs(self.local_silver_file_loc, exist_ok=True)
        
        if self.scrape_list:
            
            max_threads = 10
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                files = {executor.submit(self.minio.download_file, BUCKET, file_name['name'], os.path.join(self.local_silver_file_loc,file_name['name'].split('/')[-1])): file_name for file_name in self.scrape_list}

                for future in as_completed(files):
                    url = files[future]
                    try:
                        data = future.result()
                    except Exception as e:
                        print(f"Failed to Fetch Data")
        
        
        df = spark.read.parquet(self.local_silver_file_loc)

        self.ingest_data_with_semantic_chunking(
            df,
            DB_COLLECTION_NAME,
            CHROMA_HOST,
            CHROMA_PORT,
            EMBEDDING_MODEL
        )

        #removing all the temp files
        shutil.rmtree(f'shared_data/{self.scrape_path.split('/')[0]}')

        return True