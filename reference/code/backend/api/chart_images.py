"""
Chart Image Proxy API - Serve chart images from MinIO or local paths
"""
from fastapi import APIRouter, HTTPException, Response, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
import os
from pathlib import Path
import logging
from io import BytesIO
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from backend.core.database import get_db
from backend.models.artifact import Artifact
from backend.config.settings import settings
from webapp.services.chart_service import generate_technical_chart

logger = logging.getLogger(__name__)

# MinIO client setup
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
    minio_client = Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE
    )
except ImportError:
    MINIO_AVAILABLE = False
    minio_client = None
    class S3Error(Exception):
        """Fallback S3Error when MinIO client is unavailable."""
        pass
    logger.warning("MinIO package not available")

router = APIRouter()

IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg')


class ChartSnapshotMissingError(RuntimeError):
    """Raised when an artifact lacks the market data snapshot required for regeneration."""


class ChartRegenerationFailedError(RuntimeError):
    """Raised when regeneration fails for reasons other than a missing snapshot."""


def _build_dataframe_from_snapshot(snapshot: Dict[str, Any]) -> Optional[pd.DataFrame]:
    bars = snapshot.get('bars') if snapshot else None
    if not bars:
        return None
    df = pd.DataFrame(bars)
    if df.empty:
        return None
    if 'date' in df.columns:
        df['Date'] = pd.to_datetime(df['date'])
    elif 'timestamp' in df.columns:
        df['Date'] = pd.to_datetime(df['timestamp'])
    else:
        return None
    rename_map = {
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    for source, target in rename_map.items():
        if source in df.columns:
            df[target] = pd.to_numeric(df[source], errors='coerce')
    if not {'Open', 'High', 'Low', 'Close', 'Volume'}.issubset(df.columns):
        return None
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].sort_values('Date')
    df['Volume'] = df['Volume'].fillna(0)
    return df


def _upload_chart_bytes(data: bytes, artifact: Artifact) -> Optional[str]:
    if not (MINIO_AVAILABLE and minio_client):
        return None
    bucket_name = getattr(settings, 'MINIO_BUCKET_CHARTS', None) or settings.MINIO_BUCKET
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except S3Error as exc:
        logger.warning(f"Unable to ensure bucket {bucket_name}: {exc}")
        return None
    symbol = artifact.symbol or 'unknown'
    chart_type = artifact.chart_type or 'chart'
    object_name = f"regenerated/{symbol}/{chart_type}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_artifact_{artifact.id}.jpeg"
    data_stream = BytesIO(data)
    data_stream.seek(0)
    try:
        minio_client.put_object(
            bucket_name,
            object_name,
            data_stream,
            length=len(data),
            content_type='image/jpeg'
        )
    except Exception as exc:
        logger.warning(f"Failed to upload regenerated chart to MinIO: {exc}")
        return None
    public_endpoint = settings.MINIO_PUBLIC_ENDPOINT.rstrip('/')
    return f"http://{public_endpoint}/{bucket_name}/{object_name}"


def _persist_chart_locally(data: bytes, artifact: Artifact) -> Path:
    charts_dir = Path(getattr(settings, 'CHARTS_DIR', '/app/charts'))
    charts_dir.mkdir(parents=True, exist_ok=True)
    symbol = (artifact.symbol or 'chart').replace('/', '_')
    file_name = f"regenerated_{symbol}_{artifact.id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpeg"
    file_path = charts_dir / file_name
    file_path.write_bytes(data)
    return file_path


def _regenerate_chart_from_metadata(artifact: Artifact, db: Session) -> bytes:
    metadata = artifact.artifact_metadata or {}
    snapshot = metadata.get('market_data_snapshot') if isinstance(metadata, dict) else None
    df = _build_dataframe_from_snapshot(snapshot or {})
    if df is None:
        logger.warning(f"No market data snapshot available to regenerate artifact {artifact.id}")
        raise ChartSnapshotMissingError("Artifact is missing market_data_snapshot bars required for regeneration.")
    symbol = snapshot.get('symbol') if snapshot else artifact.symbol
    try:
        buffer = generate_technical_chart(
            df,
            symbol or artifact.symbol or 'Instrument',
            getattr(settings, 'CHART_WIDTH', 1920),
            getattr(settings, 'CHART_HEIGHT', 1080)
        )
    except Exception as exc:
        logger.error(f"Failed to regenerate chart for artifact {artifact.id}: {exc}")
        raise ChartRegenerationFailedError(f"Unable to regenerate chart image: {exc}") from exc
    data = buffer.read()
    public_url = _upload_chart_bytes(data, artifact)
    updated = False
    if public_url:
        artifact.image_path = public_url
        if isinstance(artifact.chart_data, dict):
            artifact.chart_data['minio_url'] = public_url
        else:
            artifact.chart_data = {'minio_url': public_url}
        updated = True
    else:
        try:
            local_path = _persist_chart_locally(data, artifact)
            artifact.image_path = str(local_path)
            if isinstance(artifact.chart_data, dict):
                artifact.chart_data['local_path'] = str(local_path)
            else:
                artifact.chart_data = {'local_path': str(local_path)}
            updated = True
        except Exception as exc:
            logger.warning(f"Failed to persist regenerated chart locally for artifact {artifact.id}: {exc}")
    if updated:
        try:
            db.commit()
        except Exception as exc:
            logger.warning(f"Failed to persist regenerated chart path for artifact {artifact.id}: {exc}")
            db.rollback()
        else:
            db.refresh(artifact)
    return data


def _is_supported_image(path_value: str) -> bool:
    sanitized = (path_value or '').split('?')[0].lower()
    return sanitized.endswith(IMAGE_EXTENSIONS)


def _collect_candidate_paths(file_path: Path) -> Iterable[Path]:
    shared_volume_path = Path('/app/charts')
    candidates = [
        shared_volume_path / file_path.name,
        shared_volume_path / file_path,
        file_path,
        Path('/opt/airflow') / file_path,
        Path('/opt/airflow') / file_path.name,
        Path('/tmp') / file_path.name,
    ]
    if file_path.is_absolute():
        candidates.extend([
            Path('/opt/airflow/dags') / file_path.name,
            Path('/opt/airflow/logs') / file_path.name,
        ])
    return candidates


def _locate_chart_file(
    original_path: Optional[str],
    artifact: Artifact,
    allowed_extensions: Optional[Iterable[str]] = None
) -> Optional[Path]:
    if not original_path:
        return None
    file_path = Path(original_path)
    allowed = tuple(ext.lower() for ext in allowed_extensions) if allowed_extensions else None
    for candidate in _collect_candidate_paths(file_path):
        if allowed and candidate.suffix.lower() not in allowed:
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    if artifact.symbol and artifact.chart_type:
        shared_volume_path = Path('/app/charts')
        timeframe_map = {'daily': '1D', 'weekly': '1W', 'monthly': '1M'}
        timeframe = artifact.chart_type.lower()
        timeframe_code = timeframe_map.get(timeframe, timeframe[:1].upper())
        patterns = []
        if allowed:
            for ext in allowed:
                patterns.append(f"{artifact.symbol}_{timeframe_code}*{ext}")
        else:
            patterns.append(f"{artifact.symbol}_{timeframe_code}_*")
        for pattern in patterns:
            matching_files = list(shared_volume_path.glob(pattern))
            if matching_files:
                return max(matching_files, key=lambda p: p.stat().st_mtime)
    return None


@router.get("/{artifact_id}/image")
async def get_chart_image(
    artifact_id: int,
    format: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Serve the chart artifact image, regenerating it when only HTML fallback exists."""
    try:
        artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        if artifact.type != 'chart':
            raise HTTPException(status_code=400, detail="Artifact is not a chart")
        if not artifact.image_path:
            raise HTTPException(status_code=404, detail="Chart image path not found")

        requested_format = (format or '').lower()
        if requested_format not in ('', 'html'):
            raise HTTPException(status_code=400, detail="Unsupported format requested")

        stored_path = artifact.image_path
        requesting_html = requested_format == 'html'

        # Allow direct HTML download when explicitly requested
        if requesting_html:
            if stored_path.startswith('http') and stored_path.lower().endswith('.html'):
                return Response(status_code=307, headers={'Location': stored_path})
            html_file = _locate_chart_file(stored_path, artifact, allowed_extensions=('.html',))
            if html_file:
                return FileResponse(
                    str(html_file),
                    media_type='text/html',
                    filename=html_file.name
                )
            raise HTTPException(status_code=404, detail="HTML fallback file is not accessible on the server")

        # Prefer MinIO URL when it points to an actual image
        minio_url = None
        if artifact.chart_data and isinstance(artifact.chart_data, dict):
            minio_url = artifact.chart_data.get('minio_url')
        image_url = minio_url or stored_path
        if image_url and image_url.startswith('http') and _is_supported_image(image_url):
            try:
                url_parts = image_url.replace('http://', '').replace('https://', '').split('/', 2)
                if len(url_parts) < 3:
                    raise ValueError("Invalid MinIO URL format")
                bucket_name = url_parts[1]
                object_name = url_parts[2]
                logger.info(f"Fetching chart from MinIO: bucket={bucket_name}, object={object_name}")
                if not (MINIO_AVAILABLE and minio_client):
                    raise HTTPException(status_code=503, detail="MinIO client not available")
                response = minio_client.get_object(bucket_name, object_name)
                image_data = response.read()
                response.close()
                response.release_conn()
                extension = object_name.split('.')[-1]
                content_type = 'image/jpeg' if object_name.lower().endswith(('.jpg', '.jpeg')) else 'image/png'
                return Response(
                    content=image_data,
                    media_type=content_type,
                    headers={
                        'Content-Disposition': f'inline; filename="chart-{artifact_id}.{extension}"',
                        'Cache-Control': 'public, max-age=3600'
                    }
                )
            except HTTPException:
                raise
            except Exception as exc:
                logger.error(f"Failed to fetch chart from MinIO: {exc}")
                logger.info("Falling back to local lookup/regeneration")
        elif image_url and image_url.startswith('http'):
            logger.info("Stored chart URL is not an image (likely HTML fallback). Regenerating from snapshot.")

        # Local lookup (only for image files)
        found_path = None
        if _is_supported_image(stored_path):
            found_path = _locate_chart_file(stored_path, artifact, allowed_extensions=IMAGE_EXTENSIONS)
            if found_path:
                media_type = 'image/jpeg' if found_path.suffix.lower() in ('.jpeg', '.jpg') else 'image/png'
                return FileResponse(
                    str(found_path),
                    media_type=media_type,
                    filename=f"chart-{artifact_id}{found_path.suffix}"
                )
        else:
            logger.info("Artifact stored path is not an image; regenerating from metadata snapshot")

        # Regenerate from metadata snapshot
        try:
            regenerated_data = _regenerate_chart_from_metadata(artifact, db)
        except ChartSnapshotMissingError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ChartRegenerationFailedError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        logger.info(f"Regenerated chart for artifact {artifact_id} from metadata snapshot")
        return Response(
            content=regenerated_data,
            media_type='image/jpeg',
            headers={
                'Content-Disposition': f'inline; filename="chart-{artifact_id}.jpeg"',
                'Cache-Control': 'public, max-age=3600'
            }
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error serving chart image for artifact {artifact_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))
