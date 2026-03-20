"""
Artifacts API - OpenSpec 2 Compliant
Store and retrieve ML/LLM artifacts (inputs/outputs, charts, signals)
"""
from fastapi import APIRouter, HTTPException, Depends, Response
from sqlalchemy.orm import Session
from sqlalchemy import desc, cast, String
from backend.core.database import get_db
from backend.models.artifact import Artifact
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import json

from backend.services.mlflow_lineage import resolve_run_lineage

logger = logging.getLogger(__name__)

router = APIRouter()


def _hydrate_lineage(
    artifact: Artifact,
    artifact_dict: Dict[str, Any]
) -> bool:
    """Ensure MLflow lineage is populated for an artifact."""
    if artifact_dict.get('run_id') and artifact_dict.get('experiment_id'):
        return False
    workflow_id = artifact_dict.get('workflow_id')
    execution_id = artifact_dict.get('execution_id')
    if not workflow_id or not execution_id:
        return False
    lineage = resolve_run_lineage(workflow_id, execution_id)
    if not lineage:
        return False
    artifact.run_id = lineage.get('run_id') or artifact.run_id
    artifact.experiment_id = lineage.get('experiment_id') or artifact.experiment_id
    artifact_dict['run_id'] = artifact.run_id
    artifact_dict['experiment_id'] = artifact.experiment_id
    return True


def _normalize_market_bar(bar: Dict[str, Any]) -> Dict[str, Any]:
    """Convert mixed bar payloads into the API response schema."""
    date_value = bar.get('date') or bar.get('timestamp')
    date_str: Optional[str]
    if isinstance(date_value, (int, float)):
        # Heuristic: treat >1e12 as milliseconds
        divisor = 1000 if date_value > 1_000_000_000_000 else 1
        date_str = datetime.fromtimestamp(date_value / divisor).isoformat()
    else:
        date_str = str(date_value) if date_value else None

    def _to_float(value: Any) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _to_int(value: Any) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "date": date_str,
        "open": _to_float(bar.get('open') or bar.get('o')),
        "high": _to_float(bar.get('high') or bar.get('h')),
        "low": _to_float(bar.get('low') or bar.get('l')),
        "close": _to_float(bar.get('close') or bar.get('c')),
        "volume": _to_int(bar.get('volume') or bar.get('v')),
    }


def _market_data_from_snapshot(artifact: Artifact, limit: int) -> List[Dict[str, Any]]:
    metadata = artifact.artifact_metadata if isinstance(artifact.artifact_metadata, dict) else {}
    snapshot = metadata.get('market_data_snapshot') if isinstance(metadata, dict) else None
    bars = snapshot.get('bars') if isinstance(snapshot, dict) else None
    if not bars:
        return []
    trimmed = bars[-limit:] if len(bars) > limit else list(bars)
    trimmed = list(reversed(trimmed))
    return [_normalize_market_bar(bar) for bar in trimmed]


@router.get("/")
async def get_artifacts(
    db: Session = Depends(get_db),
    type: Optional[str] = None,
    symbol: Optional[str] = None,
    execution_id: Optional[str] = None,
    group_by: Optional[str] = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    Get all artifacts with optional filtering and grouping.
    
    Args:
        type: Filter by artifact type (llm, chart, signal, order, trade, portfolio)
        symbol: Filter by trading symbol
        execution_id: Filter by execution_id
        group_by: Group artifacts by 'execution_id' or None for flat list
        limit: Maximum number of artifacts to return
    """
    try:
        query = db.query(Artifact)
        
        if type:
            # Support filtering by artifact_type in signal_data for order/trade/portfolio
            if type in ['order', 'trade', 'portfolio']:
                # These are stored as 'signal' type with artifact_type in signal_data
                query = query.filter(Artifact.type == 'signal')
                # Note: Further filtering by signal_data would require JSON query
            else:
                query = query.filter(Artifact.type == type)
        if symbol:
            query = query.filter(Artifact.symbol == symbol)
        if execution_id:
            # Use explicit string casting to avoid type coercion issues
            query = query.filter(cast(Artifact.execution_id, String) == execution_id)
        
        artifacts = query.order_by(desc(Artifact.created_at)).limit(limit).all()
        artifacts_dict = [a.to_dict() for a in artifacts]
        lineage_dirty = False
        for orm_artifact, artifact_dict in zip(artifacts, artifacts_dict):
            if _hydrate_lineage(orm_artifact, artifact_dict):
                lineage_dirty = True
        if lineage_dirty:
            db.commit()
        
        # Filter by artifact_type in signal_data if needed
        if type in ['order', 'trade', 'portfolio']:
            artifacts_dict = [
                a for a in artifacts_dict
                if a.get('signal_data', {}).get('artifact_type') == type
            ]
        
        # Group by execution_id if requested
        if group_by == 'execution_id':
            logger.debug(f"Grouping artifacts by execution_id. Found {len(artifacts_dict)} artifacts")
            grouped = {}
            for artifact in artifacts_dict:
                exec_id = artifact.get('execution_id') or 'ungrouped'
                if exec_id not in grouped:
                    grouped[exec_id] = []
                grouped[exec_id].append(artifact)
            
            # Sort groups by most recent first
            sorted_groups = {}
            if grouped:
                for exec_id in sorted(grouped.keys(), key=lambda x: (
                    grouped[x][0].get('created_at', '') if grouped[x] else ''
                ), reverse=True):
                    sorted_groups[exec_id] = grouped[exec_id]
            
            logger.debug(f"Returning grouped structure with {len(sorted_groups)} groups")
            return {
                "artifacts": artifacts_dict,  # Still return flat list for compatibility
                "grouped": sorted_groups,  # Add grouped view (empty dict if no artifacts)
                "total": len(artifacts_dict),
                "group_count": len(sorted_groups)
            }
        
        return {
            "artifacts": artifacts_dict,
            "total": len(artifacts_dict)
        }
    except Exception as e:
        logger.error(f"Error fetching artifacts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{artifact_id}")
async def get_artifact(
    artifact_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get a single artifact by ID with hydrated context."""
    try:
        artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
        
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        
        artifact_dict = artifact.to_dict()
        if _hydrate_lineage(artifact, artifact_dict):
            db.commit()
        
        # Hydrate chart artifacts with missing context
        if artifact.type == 'chart':
            artifact_dict = _hydrate_chart_artifact(artifact_dict, db)
        
        return artifact_dict
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching artifact {artifact_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _hydrate_chart_artifact(artifact_dict: Dict[str, Any], db: Session) -> Dict[str, Any]:
    """Hydrate chart artifact with market data and LLM context if missing."""
    try:
        # Check if metadata already has market_data_snapshot
        metadata = artifact_dict.get('metadata') or {}
        
        # Hydrate market data if missing
        if not metadata.get('market_data_snapshot') and artifact_dict.get('symbol'):
            try:
                from sqlalchemy import text
                from datetime import datetime
                
                query = text("""
                    SELECT md.data->'data' as bars_data
                    FROM market_data md
                    JOIN codes c ON md.conid = c.conid
                    WHERE c.symbol = :symbol
                    ORDER BY md.created_at DESC
                    LIMIT 1
                """)
                
                result = db.execute(query, {"symbol": artifact_dict['symbol']})
                row = result.fetchone()
                
                if row and row.bars_data:
                    bars = row.bars_data if isinstance(row.bars_data, list) else []
                    bars = bars[-50:] if len(bars) > 50 else bars
                    
                    market_data_snapshot = {
                        'symbol': artifact_dict['symbol'],
                        'timeframe': artifact_dict.get('chart_type', 'unknown'),
                        'bar_count': len(bars),
                        'bars': []
                    }
                    
                    for bar in bars:
                        if isinstance(bar, dict):
                            timestamp_ms = bar.get("t") or bar.get("timestamp")
                            date_str = None
                            if timestamp_ms:
                                try:
                                    date_str = datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
                                except (ValueError, TypeError):
                                    date_str = str(timestamp_ms)
                            
                            market_data_snapshot['bars'].append({
                                "date": date_str,
                                "open": float(bar.get("o", 0)) if bar.get("o") else None,
                                "high": float(bar.get("h", 0)) if bar.get("h") else None,
                                "low": float(bar.get("l", 0)) if bar.get("l") else None,
                                "close": float(bar.get("c", 0)) if bar.get("c") else None,
                                "volume": int(bar.get("v", 0)) if bar.get("v") else None,
                            })
                    
                    if market_data_snapshot['bars']:
                        latest_bar = market_data_snapshot['bars'][-1]
                        market_data_snapshot['latest_price'] = latest_bar.get('close')
                    
                    metadata['market_data_snapshot'] = market_data_snapshot
            except Exception as e:
                logger.warning(f"Failed to hydrate market data for artifact {artifact_dict['id']}: {e}")
        
        # Hydrate LLM analysis if missing
        if not metadata.get('llm_analysis') and artifact_dict.get('symbol') and artifact_dict.get('execution_id'):
            try:
                # Find related LLM or signal artifact
                llm_artifact = db.query(Artifact).filter(
                    Artifact.symbol == artifact_dict['symbol'],
                    Artifact.execution_id == artifact_dict['execution_id'],
                    Artifact.type.in_(['llm', 'signal'])
                ).order_by(Artifact.created_at.desc()).first()
                
                if llm_artifact:
                    # Update prompt/response if missing
                    if not artifact_dict.get('prompt') and llm_artifact.prompt:
                        artifact_dict['prompt'] = llm_artifact.prompt
                        artifact_dict['prompt_length'] = len(llm_artifact.prompt)
                    
                    if not artifact_dict.get('response') and llm_artifact.response:
                        artifact_dict['response'] = llm_artifact.response
                        artifact_dict['response_length'] = len(llm_artifact.response)
                    
                    if not artifact_dict.get('model_name') and llm_artifact.model_name:
                        artifact_dict['model_name'] = llm_artifact.model_name
                    
                    # Build LLM analysis summary
                    if llm_artifact.type == 'signal' and llm_artifact.signal_data:
                        signal_data = llm_artifact.signal_data
                        metadata['llm_analysis'] = {
                            'action': llm_artifact.action or signal_data.get('action'),
                            'confidence': signal_data.get('confidence_level'),
                            'confidence_score': signal_data.get('confidence_score'),
                            'reasoning_snippet': signal_data.get('reasoning', '')[:200],
                            'key_factors': signal_data.get('key_factors', [])[:3],
                            'entry_price': signal_data.get('entry_price'),
                            'stop_loss': signal_data.get('stop_loss'),
                            'take_profit': signal_data.get('take_profit'),
                            'is_actionable': signal_data.get('is_actionable')
                        }
            except Exception as e:
                logger.warning(f"Failed to hydrate LLM analysis for artifact {artifact_dict['id']}: {e}")
        
        artifact_dict['metadata'] = metadata
        return artifact_dict
    except Exception as e:
        logger.error(f"Error hydrating chart artifact: {e}")
        return artifact_dict


@router.post("/")
async def create_artifact(
    artifact_data: Dict[str, Any],
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Create a new artifact.
    
    Expected payload:
    {
        "name": "LLM Analysis for TSLA",
        "type": "llm|chart|signal",
        "symbol": "TSLA",
        "prompt": "Analyze TSLA...",
        "response": "Based on analysis...",
        "run_id": "mlflow_run_id",
        ...
    }
    """
    try:
        # Validate required fields
        if 'name' not in artifact_data or 'type' not in artifact_data:
            raise HTTPException(status_code=400, detail="name and type are required")
        
        if artifact_data['type'] not in ['llm', 'chart', 'signal']:
            raise HTTPException(status_code=400, detail="type must be llm, chart, or signal")
        
        # Calculate lengths for LLM artifacts
        if artifact_data['type'] == 'llm':
            if 'prompt' in artifact_data:
                artifact_data['prompt_length'] = len(artifact_data['prompt'])
            if 'response' in artifact_data:
                artifact_data['response_length'] = len(artifact_data['response'])
        
        # Handle metadata field rename (metadata -> artifact_metadata)
        if 'metadata' in artifact_data:
            artifact_data['artifact_metadata'] = artifact_data.pop('metadata')
        
        # Create artifact
        artifact = Artifact(**artifact_data)
        db.add(artifact)
        db.commit()
        db.refresh(artifact)
        
        logger.info(f"Created artifact: {artifact.id} ({artifact.type})")
        
        return artifact.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating artifact: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{artifact_id}/market-data")
async def get_artifact_market_data(
    artifact_id: int,
    limit: int = 50,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get market data for a chart artifact.
    Returns OHLCV data for the artifact's symbol.
    """
    try:
        artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
        
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        
        if artifact.type != 'chart':
            raise HTTPException(status_code=400, detail="Market data is only available for chart artifacts")
        
        if not artifact.symbol:
            raise HTTPException(status_code=400, detail="Artifact does not have a symbol")
        
        # First, use the persisted market data snapshot stored with the artifact
        market_data = _market_data_from_snapshot(artifact, limit)
        if market_data:
            return {
                "symbol": artifact.symbol,
                "count": len(market_data),
                "data": market_data
            }
        
        # Fall back to the shared market_data table for legacy artifacts
        from sqlalchemy import text
        query = text("""
            SELECT 
                md.data->'data' as bars_data
            FROM market_data md
            JOIN codes c ON md.conid = c.conid
            WHERE c.symbol = :symbol
            ORDER BY md.created_at DESC
            LIMIT 1
        """)
        result = db.execute(query, {"symbol": artifact.symbol})
        row = result.fetchone()
        bars = row.bars_data if row and isinstance(row.bars_data, list) else []
        if bars:
            trimmed = bars[-limit:] if len(bars) > limit else bars
            trimmed.reverse()
            market_data = [_normalize_market_bar(bar) for bar in trimmed if isinstance(bar, dict)]
        
        if not market_data:
            logger.warning(
                "No market data available for artifact %s (%s). Snapshot missing and cache empty.",
                artifact_id,
                artifact.symbol
            )
            raise HTTPException(status_code=404, detail="No market data stored with this artifact")
        
        return {
            "symbol": artifact.symbol,
            "count": len(market_data),
            "data": market_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching market data for artifact {artifact_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{artifact_id}/download")
async def download_artifact(
    artifact_id: int,
    db: Session = Depends(get_db)
):
    """Download artifact as JSON file."""
    try:
        artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
        
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        
        # Convert to JSON
        artifact_json = json.dumps(artifact.to_dict(), indent=2)
        
        return Response(
            content=artifact_json,
            media_type="application/json",
            headers={
                "Content-Disposition": f"attachment; filename=artifact-{artifact_id}.json"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading artifact {artifact_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{artifact_id}")
async def update_artifact(
    artifact_id: int,
    artifact_updates: Dict[str, Any],
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Update an artifact (partial update)."""
    try:
        artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
        
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        
        # Handle metadata field rename
        if 'metadata' in artifact_updates:
            artifact_updates['artifact_metadata'] = artifact_updates.pop('metadata')
        
        # Update fields
        for key, value in artifact_updates.items():
            if hasattr(artifact, key):
                setattr(artifact, key, value)
        
        db.commit()
        db.refresh(artifact)
        
        logger.info(f"Updated artifact: {artifact_id}")
        
        return artifact.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating artifact {artifact_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{artifact_id}")
async def delete_artifact(
    artifact_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete an artifact."""
    try:
        artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
        
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        
        db.delete(artifact)
        db.commit()
        
        logger.info(f"Deleted artifact: {artifact_id}")
        
        return {"message": "Artifact deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting artifact {artifact_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/summary")
async def get_artifact_stats(
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get artifact statistics."""
    try:
        total = db.query(Artifact).count()
        llm_count = db.query(Artifact).filter(Artifact.type == 'llm').count()
        chart_count = db.query(Artifact).filter(Artifact.type == 'chart').count()
        signal_count = db.query(Artifact).filter(Artifact.type == 'signal').count()
        
        return {
            "total": total,
            "by_type": {
                "llm": llm_count,
                "chart": chart_count,
                "signal": signal_count
            }
        }
    except Exception as e:
        logger.error(f"Error fetching artifact stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
