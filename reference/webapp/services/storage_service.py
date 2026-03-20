from minio import Minio
from minio.error import S3Error
import io
import uuid
from webapp.config.settings import (
    logger, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
)

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket():
    """Ensure that the MinIO bucket exists."""
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created MinIO bucket: {MINIO_BUCKET}")
        return True
    except S3Error as e:
        logger.error(f"Error ensuring MinIO bucket exists: {str(e)}")
        return False

def save_buffer_to_minio(buffer, folder, file_extension="jpg", content_type="image/jpeg"):
    """Save a buffer to MinIO storage.
    
    Args:
        buffer: BytesIO buffer containing the data
        folder: Folder path within the bucket
        file_extension: File extension (default: jpg)
        content_type: MIME content type (default: image/jpeg)
        
    Returns:
        URL to the saved file or None on error
    """
    try:
        ensure_bucket()
        
        # Generate a unique filename
        filename = f"{folder}/{uuid.uuid4()}.{file_extension}"
        
        # Reset buffer position to start
        buffer.seek(0)
        
        # Upload to MinIO
        minio_client.put_object(
            MINIO_BUCKET,
            filename,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type=content_type
        )
        
        # Build URL to the file
        file_url = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
        logger.debug(f"Saved file to MinIO: {file_url}")
        
        return file_url
    except S3Error as e:
        logger.error(f"MinIO error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error saving to MinIO: {str(e)}")
        return None

def delete_from_minio(filepath):
    """Delete a file from MinIO storage.
    
    Args:
        filepath: Path within the bucket
        
    Returns:
        Success status (boolean)
    """
    try:
        ensure_bucket()
        minio_client.remove_object(MINIO_BUCKET, filepath)
        logger.debug(f"Deleted file from MinIO: {filepath}")
        return True
    except S3Error as e:
        logger.error(f"MinIO error deleting file {filepath}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error deleting from MinIO: {str(e)}")
        return False

def list_files_in_minio(prefix=""):
    """List files in MinIO bucket with optional prefix.
    
    Args:
        prefix: Folder/prefix to filter results
        
    Returns:
        List of file objects
    """
    try:
        ensure_bucket()
        objects = minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        return list(objects)
    except S3Error as e:
        logger.error(f"MinIO error listing files: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Error listing MinIO files: {str(e)}")
        return [] 