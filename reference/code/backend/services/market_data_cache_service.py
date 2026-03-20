"""Market Data Cache Service for managing cached IBKR data."""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from backend.models.market_data_cache import MarketDataCache
from backend.services.ibkr_service import IBKRService
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class MarketDataCacheService:
    """
    Service for managing market data cache.
    
    Provides methods to fetch data with caching, populate cache,
    and manage cached data lifecycle.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.ibkr = IBKRService()
        self.cache_enabled = settings.CACHE_ENABLED
        self.debug_mode = settings.DEBUG_MODE
        self.cache_ttl_hours = settings.CACHE_TTL_HOURS
    
    async def get_or_fetch_market_data(
        self,
        symbol: str,
        exchange: str = "NASDAQ",
        data_type: str = "daily",
        timeframe: str = "1d",
        period: str = "1y",
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Get market data from cache or fetch from IBKR if not cached/expired.
        
        Args:
            symbol: Stock symbol (e.g., "NVDA")
            exchange: Exchange name (default: "NASDAQ")
            data_type: Type of data (daily, weekly, monthly, intraday)
            timeframe: Timeframe for data (1d, 1w, 1m)
            period: Period to fetch (1y, 6m, 1m, etc.)
            force_refresh: Force fetch from IBKR even if cached
            
        Returns:
            Dictionary with market data and metadata
        """
        # In debug mode, only use cache
        if self.debug_mode and not force_refresh:
            logger.info(f"DEBUG MODE: Using cached data for {symbol}")
            cached_data = self._get_from_cache(symbol, exchange, data_type)
            if cached_data:
                return {
                    "data": cached_data.ohlcv_data,
                    "source": "cache",
                    "cached_at": cached_data.cached_at.isoformat(),
                    "data_timestamp": cached_data.data_timestamp.isoformat(),
                    "symbol": symbol,
                    "exchange": exchange,
                }
            else:
                raise ValueError(
                    f"Debug mode enabled but data not in cache for {symbol} ({exchange}). "
                    f"Run populate_market_cache.py script to cache data."
                )
        
        # Check cache first if enabled and not forcing refresh
        if self.cache_enabled and not force_refresh:
            cached_data = self._get_from_cache(symbol, exchange, data_type)
            if cached_data and not cached_data.is_expired():
                logger.info(f"Cache HIT for {symbol} ({data_type})")
                return {
                    "data": cached_data.ohlcv_data,
                    "source": "cache",
                    "cached_at": cached_data.cached_at.isoformat(),
                    "data_timestamp": cached_data.data_timestamp.isoformat(),
                    "symbol": symbol,
                    "exchange": exchange,
                }
            else:
                logger.info(f"Cache MISS or EXPIRED for {symbol} ({data_type})")
        
        # Fetch from IBKR
        logger.info(f"Fetching live data from IBKR for {symbol}")
        market_data = await self._fetch_from_ibkr(symbol, period, timeframe)
        
        # Cache the fetched data if caching is enabled
        if self.cache_enabled:
            await self._save_to_cache(
                symbol=symbol,
                exchange=exchange,
                data_type=data_type,
                timeframe=timeframe,
                ohlcv_data=market_data,
                conid=market_data.get("conid")
            )
        
        return {
            "data": market_data,
            "source": "live_api",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "exchange": exchange,
        }
    
    def _get_from_cache(
        self,
        symbol: str,
        exchange: str,
        data_type: str
    ) -> Optional[MarketDataCache]:
        """Get most recent cached data for symbol."""
        return self.db.query(MarketDataCache).filter(
            and_(
                MarketDataCache.symbol == symbol,
                MarketDataCache.exchange == exchange,
                MarketDataCache.data_type == data_type,
                MarketDataCache.is_active == True
            )
        ).order_by(desc(MarketDataCache.data_timestamp)).first()
    
    async def _fetch_from_ibkr(
        self,
        symbol: str,
        period: str = "1y",
        bar: str = "1d"
    ) -> Dict[str, Any]:
        """Fetch historical data from IBKR."""
        # First, search for the contract
        contracts = await self.ibkr.search_contracts(symbol)
        if not contracts or len(contracts) == 0:
            raise ValueError(f"Symbol {symbol} not found in IBKR")
        
        # Get the first matching contract (prefer stocks)
        contract = contracts[0]
        conid = contract.get("conid")
        
        # Fetch historical data
        hist_data = await self.ibkr.get_historical_data(
            conid=conid,
            period=period,
            bar=bar
        )
        
        # Add conid to the response
        hist_data["conid"] = conid
        return hist_data
    
    async def _save_to_cache(
        self,
        symbol: str,
        exchange: str,
        data_type: str,
        timeframe: str,
        ohlcv_data: Dict[str, Any],
        conid: Optional[int] = None
    ) -> MarketDataCache:
        """Save market data to cache."""
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=self.cache_ttl_hours)
        
        # Use the latest timestamp from the data or current time
        data_timestamp = now
        if isinstance(ohlcv_data.get("data"), list) and len(ohlcv_data["data"]) > 0:
            # Get the last data point's timestamp
            last_point = ohlcv_data["data"][-1]
            if "t" in last_point:
                data_timestamp = datetime.fromtimestamp(last_point["t"] / 1000, tz=timezone.utc)
        
        # Check if entry already exists
        existing = self.db.query(MarketDataCache).filter(
            and_(
                MarketDataCache.symbol == symbol,
                MarketDataCache.exchange == exchange,
                MarketDataCache.data_type == data_type,
                MarketDataCache.data_timestamp == data_timestamp
            )
        ).first()
        
        if existing:
            # Update existing entry
            existing.ohlcv_data = ohlcv_data
            existing.cached_at = now
            existing.expires_at = expires_at
            existing.timeframe = timeframe
            existing.conid = conid
            cache_entry = existing
        else:
            # Create new entry
            cache_entry = MarketDataCache(
                symbol=symbol,
                exchange=exchange,
                conid=conid,
                data_type=data_type,
                timeframe=timeframe,
                ohlcv_data=ohlcv_data,
                source="IBKR",
                cached_at=now,
                expires_at=expires_at,
                data_timestamp=data_timestamp,
                extra_metadata={"period": ohlcv_data.get("period"), "bar": ohlcv_data.get("bar")}
            )
            self.db.add(cache_entry)
        
        self.db.commit()
        self.db.refresh(cache_entry)
        
        logger.info(f"Cached data for {symbol} ({data_type}), expires at {expires_at}")
        return cache_entry
    
    def get_cached_symbols(self) -> List[str]:
        """Get list of all symbols in cache."""
        results = self.db.query(MarketDataCache.symbol).distinct().all()
        return [r[0] for r in results]
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self.db.query(MarketDataCache).count()
        active = self.db.query(MarketDataCache).filter(MarketDataCache.is_active == True).count()
        expired = self.db.query(MarketDataCache).filter(
            and_(
                MarketDataCache.is_active == True,
                MarketDataCache.expires_at < datetime.now(timezone.utc)
            )
        ).count()
        
        symbols = self.get_cached_symbols()
        
        return {
            "total_entries": total,
            "active_entries": active,
            "expired_entries": expired,
            "unique_symbols": len(symbols),
            "symbols": symbols,
            "cache_enabled": self.cache_enabled,
            "debug_mode": self.debug_mode,
            "ttl_hours": self.cache_ttl_hours,
        }
    
    def clear_expired(self) -> int:
        """Clear expired cache entries."""
        count = self.db.query(MarketDataCache).filter(
            and_(
                MarketDataCache.is_active == True,
                MarketDataCache.expires_at < datetime.now(timezone.utc)
            )
        ).update({"is_active": False})
        
        self.db.commit()
        logger.info(f"Cleared {count} expired cache entries")
        return count
    
    def clear_symbol(self, symbol: str) -> int:
        """Clear all cache entries for a specific symbol."""
        count = self.db.query(MarketDataCache).filter(
            MarketDataCache.symbol == symbol
        ).update({"is_active": False})
        
        self.db.commit()
        logger.info(f"Cleared {count} cache entries for {symbol}")
        return count

