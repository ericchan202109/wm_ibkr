"""Market Data Cache API endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel

from backend.core.database import get_db
from backend.services.market_data_cache_service import MarketDataCacheService
from backend.models.market_data_cache import MarketDataCache
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class CacheRefreshRequest(BaseModel):
    """Request model for cache refresh."""
    symbols: List[str]
    exchange: str = "NASDAQ"
    force: bool = False


class CacheRefreshResponse(BaseModel):
    """Response model for cache refresh."""
    refreshed: List[str]
    failed: List[dict]
    timestamp: str


@router.get("/cache-stats")
async def get_cache_stats(db: Session = Depends(get_db)):
    """Get cache statistics."""
    try:
        cache_service = MarketDataCacheService(db)
        return cache_service.get_cache_stats()
        
    except Exception as e:
        logger.error(f"Failed to get cache stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cache")
async def list_cached_data(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    date_from: Optional[str] = Query(None, description="Filter from date (ISO format)"),
    date_to: Optional[str] = Query(None, description="Filter to date (ISO format)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    List cached market data entries.
    
    Returns paginated list of cached data with metadata.
    """
    try:
        cache_service = MarketDataCacheService(db)
        
        # Build query
        query = db.query(MarketDataCache).filter(MarketDataCache.is_active == True)
        
        if symbol:
            query = query.filter(MarketDataCache.symbol == symbol)
        
        # TODO: Add date filtering if needed
        
        # Get total count
        total = query.count()
        
        # Get paginated results
        entries = query.order_by(MarketDataCache.data_timestamp.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "entries": [entry.to_dict() for entry in entries]
        }
        
    except Exception as e:
        logger.error(f"Failed to list cached data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cache/{symbol}")
async def get_cached_symbol_data(
    symbol: str,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    data_type: Optional[str] = Query("daily"),
    db: Session = Depends(get_db)
):
    """
    Get all cached data for a specific symbol.
    """
    try:
        query = db.query(MarketDataCache).filter(
            MarketDataCache.symbol == symbol,
            MarketDataCache.is_active == True
        )
        
        if data_type:
            query = query.filter(MarketDataCache.data_type == data_type)
        
        entries = query.order_by(MarketDataCache.data_timestamp.desc()).all()
        
        if not entries:
            raise HTTPException(status_code=404, detail=f"No cached data found for symbol: {symbol}")
        
        return {
            "symbol": symbol,
            "count": len(entries),
            "data": [entry.to_dict() for entry in entries]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get cached data for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cache/refresh", response_model=CacheRefreshResponse)
async def refresh_cached_data(
    request: CacheRefreshRequest,
    db: Session = Depends(get_db)
):
    """
    Refresh cached data for specified symbols.
    
    Fetches latest data from IBKR and updates cache.
    """
    try:
        from datetime import datetime, timezone
        
        cache_service = MarketDataCacheService(db)
        
        refreshed = []
        failed = []
        
        for symbol in request.symbols:
            try:
                result = await cache_service.get_or_fetch_market_data(
                    symbol=symbol,
                    exchange=request.exchange,
                    data_type="daily",
                    timeframe="1d",
                    period="1y",
                    force_refresh=request.force
                )
                refreshed.append(symbol)
                logger.info(f"Refreshed cache for {symbol}: {result.get('source')}")
                
            except Exception as e:
                logger.error(f"Failed to refresh {symbol}: {str(e)}")
                failed.append({"symbol": symbol, "error": str(e)})
        
        return CacheRefreshResponse(
            refreshed=refreshed,
            failed=failed,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
    except Exception as e:
        logger.error(f"Cache refresh failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/cache")
async def clear_cache(
    symbol: Optional[str] = Query(None, description="Clear specific symbol (if not provided, clears expired)"),
    days_old: Optional[int] = Query(None, ge=1, description="Clear data older than N days"),
    db: Session = Depends(get_db)
):
    """
    Clear cache entries.
    
    - If symbol provided: clear all entries for that symbol
    - If days_old provided: clear entries older than N days
    - If neither provided: clear only expired entries
    """
    try:
        cache_service = MarketDataCacheService(db)
        
        if symbol:
            deleted = cache_service.clear_symbol(symbol)
            message = f"Cleared cache for symbol: {symbol}"
        else:
            deleted = cache_service.clear_expired()
            message = "Cleared expired cache entries"
        
        stats = cache_service.get_cache_stats()
        
        return {
            "deleted": deleted,
            "remaining": stats["active_entries"],
            "message": message
        }
        
    except Exception as e:
        logger.error(f"Failed to clear cache: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Moved /cache-stats above to avoid route conflict with /cache/{symbol}

