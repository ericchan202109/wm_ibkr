"""MinIO storage service for chart images."""
from minio import Minio
from minio.error import S3Error
import io
from typing import BinaryIO
from backend.config.settings import settings
import logging

logger = logging.getLogger(__name__)


class StorageService:
    """Service for MinIO object storage operations."""
    
    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        self.bucket = settings.MINIO_BUCKET
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Ensure bucket exists, create if not."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created MinIO bucket: {self.bucket}")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise
    
    def upload_chart(self, file_data: bytes, object_name: str, content_type: str = "image/png") -> str:
        """
        Upload chart image to MinIO.
        
        Args:
            file_data: Image file bytes
            object_name: Object name/path in bucket
            content_type: Content type (default: image/png)
            
        Returns:
            URL to access the object
        """
        try:
            file_stream = io.BytesIO(file_data)
            self.client.put_object(
                self.bucket,
                object_name,
                file_stream,
                length=len(file_data),
                content_type=content_type
            )
            
            # Generate URL
            url = f"http://{settings.MINIO_ENDPOINT}/{self.bucket}/{object_name}"
            logger.info(f"Uploaded chart to MinIO: {object_name}")
            return url
            
        except S3Error as e:
            logger.error(f"Failed to upload chart: {e}")
            raise
    
    def get_chart(self, object_name: str) -> bytes:
        """
        Retrieve chart image from MinIO.
        
        Args:
            object_name: Object name/path in bucket
            
        Returns:
            Image file bytes
        """
        try:
            response = self.client.get_object(self.bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Failed to get chart: {e}")
            raise
    
    def delete_chart(self, object_name: str):
        """Delete chart from MinIO."""
        try:
            self.client.remove_object(self.bucket, object_name)
            logger.info(f"Deleted chart from MinIO: {object_name}")
        except S3Error as e:
            logger.error(f"Failed to delete chart: {e}")
            raise
    
    def list_charts(self, prefix: str = "") -> list:
        """List charts in bucket with optional prefix."""
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Failed to list charts: {e}")
            raise

