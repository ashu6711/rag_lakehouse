
import os
import json
import logging
from typing import Optional, List, Dict, Any, Union, BinaryIO
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

from minio import Minio
from minio.error import S3Error, InvalidResponseError
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from urllib3.exceptions import MaxRetryError


class MinIOHelper:
    """
    A comprehensive helper class for MinIO operations.
    
    Provides methods for storing, fetching, deleting, and managing files in MinIO.
    """
    
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        region: Optional[str] = None,
        http_client: Optional[Any] = None
    ):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            secure: Whether to use HTTPS (default: True)
            region: Region name (optional)
            http_client: Custom HTTP client (optional)
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
            http_client=http_client
        )
        self.logger = logging.getLogger(__name__)
    
    def create_bucket(self, bucket_name: str, region: Optional[str] = None) -> bool:
        """
        Create a new bucket.
        
        Args:
            bucket_name: Name of the bucket to create
            region: Region for the bucket (optional)
            
        Returns:
            True if bucket was created or already exists, False otherwise
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name, location=region)
                self.logger.info(f"Bucket '{bucket_name}' created successfully")
            else:
                self.logger.info(f"Bucket '{bucket_name}' already exists")
            return True
        except S3Error as e:
            self.logger.error(f"Error creating bucket '{bucket_name}': {e}")
            return False

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload a file to MinIO.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in MinIO
            file_path: Local file path
            content_type: Content type of the file (optional)
            metadata: Additional metadata (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_size = os.path.getsize(file_path)
            
            with open(file_path, 'rb') as file_data:
                self.client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=file_data,
                    length=file_size,
                    content_type=content_type,
                    metadata=metadata
                )
            
            self.logger.info(f"File '{file_path}' uploaded as '{object_name}' to bucket '{bucket_name}'")
            return True
        except (S3Error, OSError) as e:
            self.logger.error(f"Error uploading file '{file_path}': {e}")
            return False
    
    def upload_data(
        self,
        bucket_name: str,
        object_name: str,
        data: Union[str, bytes, BinaryIO],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload data directly to MinIO.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in MinIO
            data: Data to upload (string, bytes, or file-like object)
            content_type: Content type of the data (optional)
            metadata: Additional metadata (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if isinstance(data, str):
                data = data.encode('utf-8')
            
            if isinstance(data, bytes):
                data = BytesIO(data)
                length = len(data.getvalue())
            else:
                # File-like object
                length = -1
            
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=data,
                length=length,
                content_type=content_type,
                metadata=metadata
            )
            
            self.logger.info(f"Data uploaded as '{object_name}' to bucket '{bucket_name}'")
            return True
        except S3Error as e:
            self.logger.error(f"Error uploading data to '{object_name}': {e}")
            return False
    
    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str
    ) -> bool:
        """
        Download a file from MinIO.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in MinIO
            file_path: Local file path to save the downloaded file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            self.client.fget_object(bucket_name, object_name, file_path)
            self.logger.info(f"Object '{object_name}' downloaded to '{file_path}'")
            return True
        except S3Error as e:
            self.logger.error(f"Error downloading object '{object_name}': {e}")
            return False
    
    def get_object_data(self, bucket_name: str, object_name: str) -> Optional[bytes]:
        """
        Get object data as bytes.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in MinIO
            
        Returns:
            Object data as bytes, or None if error
        """
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            self.logger.error(f"Error getting object data '{object_name}': {e}")
            return None

    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete an object from MinIO.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            self.logger.info(f"Object '{object_name}' deleted from bucket '{bucket_name}'")
            return True
        except S3Error as e:
            self.logger.error(f"Error deleting object '{object_name}': {e}")
            return False
    
    def delete_objects(self, bucket_name: str, object_names: List[str]) -> bool:
        """
        Delete multiple objects from MinIO.
        
        Args:
            bucket_name: Name of the bucket
            object_names: List of object names to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            delete_objects = [DeleteObject(name) for name in object_names]
            errors = self.client.remove_objects(bucket_name, delete_objects)
            
            error_list = list(errors)
            if error_list:
                for error in error_list:
                    self.logger.error(f"Error deleting object '{error.object_name}': {error}")
                return False
            
            self.logger.info(f"Deleted {len(object_names)} objects from bucket '{bucket_name}'")
            return True
        except S3Error as e:
            self.logger.error(f"Error deleting objects: {e}")
            return False
    
    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = True,
        include_metadata: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List objects in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Object name prefix filter
            recursive: Whether to list recursively
            include_metadata: Whether to include object metadata
            
        Returns:
            List of object information dictionaries
        """
        try:
            objects = self.client.list_objects(
                bucket_name,
                prefix=prefix,
                recursive=recursive
            )
            
            result = []
            for obj in objects:
                obj_info = {
                    'name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified,
                    'etag': obj.etag,
                    'content_type': obj.content_type
                }
                
                if include_metadata:
                    try:
                        stat = self.client.stat_object(bucket_name, obj.object_name)
                        obj_info['metadata'] = stat.metadata
                    except S3Error:
                        obj_info['metadata'] = {}
                
                result.append(obj_info)
            
            return result
        except S3Error as e:
            self.logger.error(f"Error listing objects in bucket '{bucket_name}': {e}")
            return []

