"""MinIO storage service for chart images and files."""
import io
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
import uuid

from minio import Minio
from minio.error import S3Error

from backend.config.settings import settings

logger = logging.getLogger(__name__)


class MinIOService:
    """Service for managing chart storage in MinIO."""
    
    def __init__(self):
        """Initialize MinIO client."""
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        self.bucket_name = settings.MINIO_BUCKET_CHARTS
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the charts bucket exists and is publicly readable."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created MinIO bucket: {self.bucket_name}")
                
                # Set bucket policy to allow public read access for charts
                policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "*"},
                            "Action": ["s3:GetObject"],
                            "Resource": [f"arn:aws:s3:::{self.bucket_name}/*"]
                        }
                    ]
                }
                import json
                self.client.set_bucket_policy(self.bucket_name, json.dumps(policy))
                logger.info(f"Set public read policy for bucket: {self.bucket_name}")
            else:
                logger.debug(f"MinIO bucket exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise
    
    async def upload_chart(
        self,
        chart_jpeg: bytes,
        chart_html: str,
        symbol: str,
        chart_id: int,
        timeframe: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Upload chart files to MinIO.
        
        Args:
            chart_jpeg: JPEG chart bytes
            chart_html: HTML chart string
            symbol: Stock symbol
            chart_id: Chart database ID
            
        Returns:
            Tuple of (JPEG URL, HTML URL)
        """
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]

            # Map timeframe to human-readable alias directory
            alias = None
            if timeframe:
                tf = timeframe.lower()
                if tf in ("1d", "d", "day", "daily"):
                    alias = "daily"
                elif tf in ("1w", "w", "week", "weekly"):
                    alias = "weekly"
                elif tf in ("1mo", "1m", "mo", "month", "monthly"):
                    alias = "monthly"
            
            # Generate object names (optionally include timeframe alias folder)
            base_dir = f"charts/{symbol}" + (f"/{alias}" if alias else "")
            jpeg_name = f"{base_dir}/{timestamp}_{unique_id}_{chart_id}.jpg"
            html_name = f"{base_dir}/{timestamp}_{unique_id}_{chart_id}.html"
            
            # Upload JPEG
            jpeg_stream = io.BytesIO(chart_jpeg)
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=jpeg_name,
                data=jpeg_stream,
                length=len(chart_jpeg),
                content_type="image/jpeg"
            )
            
            # Upload HTML
            html_bytes = chart_html.encode('utf-8')
            html_stream = io.BytesIO(html_bytes)
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=html_name,
                data=html_stream,
                length=len(html_bytes),
                content_type="text/html"
            )
            
            # Generate URLs
            jpeg_url = f"{self._get_base_url()}/{self.bucket_name}/{jpeg_name}"
            html_url = f"{self._get_base_url()}/{self.bucket_name}/{html_name}"
            
            logger.info(f"Uploaded chart {chart_id} for {symbol} to MinIO")
            return jpeg_url, html_url
            
        except S3Error as e:
            logger.error(f"Error uploading chart to MinIO: {e}")
            raise
    
    async def get_chart(self, object_name: str) -> bytes:
        """
        Retrieve chart file from MinIO.
        
        Args:
            object_name: Object name in MinIO
            
        Returns:
            Chart file bytes
        """
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Error retrieving chart from MinIO: {e}")
            raise
    
    async def delete_chart(self, jpeg_url: str, html_url: str):
        """
        Delete chart files from MinIO.
        
        Args:
            jpeg_url: JPEG chart URL
            html_url: HTML chart URL
        """
        try:
            # Extract object names from URLs
            jpeg_name = self._extract_object_name(jpeg_url)
            html_name = self._extract_object_name(html_url)
            
            if jpeg_name:
                self.client.remove_object(self.bucket_name, jpeg_name)
                logger.info(f"Deleted chart JPEG: {jpeg_name}")
            
            if html_name:
                self.client.remove_object(self.bucket_name, html_name)
                logger.info(f"Deleted chart HTML: {html_name}")
                
        except S3Error as e:
            logger.error(f"Error deleting chart from MinIO: {e}")
            raise
    
    async def cleanup_expired_charts(self, expiry_days: int = 30):
        """
        Delete charts older than expiry_days.
        
        Args:
            expiry_days: Number of days to keep charts
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=expiry_days)
            objects = self.client.list_objects(self.bucket_name, prefix="charts/", recursive=True)
            
            deleted_count = 0
            for obj in objects:
                if obj.last_modified < cutoff_date:
                    self.client.remove_object(self.bucket_name, obj.object_name)
                    deleted_count += 1
                    logger.debug(f"Deleted expired chart: {obj.object_name}")
            
            logger.info(f"Cleaned up {deleted_count} expired charts from MinIO")
            return deleted_count
            
        except S3Error as e:
            logger.error(f"Error cleaning up expired charts: {e}")
            raise
    
    def _get_base_url(self) -> str:
        """
        Get the base URL for browser-accessible MinIO URLs.
        
        Uses MINIO_PUBLIC_ENDPOINT which may differ from MINIO_ENDPOINT
        to support Docker deployments where internal and external hostnames differ.
        """
        protocol = "https" if settings.MINIO_SECURE else "http"
        return f"{protocol}://{settings.MINIO_PUBLIC_ENDPOINT}"
    
    def _extract_object_name(self, url: str) -> Optional[str]:
        """Extract object name from MinIO URL."""
        try:
            # URL format: http://minio:9000/bucket/object/name
            parts = url.split(f"/{self.bucket_name}/")
            if len(parts) == 2:
                return parts[1]
            return None
        except Exception as e:
            logger.error(f"Error extracting object name from URL {url}: {e}")
            return None
    
    async def get_presigned_url(self, object_name: str, expires_hours: int = 24) -> str:
        """
        Get a presigned URL for temporary access to a chart.
        
        Args:
            object_name: Object name in MinIO
            expires_hours: Hours until URL expires
            
        Returns:
            Presigned URL
        """
        try:
            from datetime import timedelta
            url = self.client.presigned_get_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                expires=timedelta(hours=expires_hours)
            )
            return url
        except S3Error as e:
            logger.error(f"Error generating presigned URL: {e}")
            raise

