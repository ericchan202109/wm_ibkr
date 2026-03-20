"""Symbol search and caching service."""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import or_
from backend.models.symbol import Symbol
from backend.services.ibkr_service import IBKRService

logger = logging.getLogger(__name__)


class SymbolService:
    """
    Service for searching and caching IBKR symbols.
    
    This service provides:
    - Symbol search with automatic caching
    - Cache invalidation and refresh
    - Batch operations
    - Contract details retrieval
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.ibkr = IBKRService()
        self.cache_ttl_days = 7  # Cache symbols for 7 days
    
    async def search_symbols(
        self,
        query: str,
        limit: int = 10,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Search for symbols by symbol or company name.
        
        Args:
            query: Search query (symbol or company name)
            limit: Maximum results to return
            use_cache: Whether to check cache first
            
        Returns:
            List of symbol dictionaries with contract details
        """
        results = []
        
        # 1. Check cache if enabled
        if use_cache:
            cached_results = self._search_cache(query, limit)
            if cached_results:
                logger.info(f"Symbol search '{query}' found {len(cached_results)} cached results")
                return [s.to_dict() for s in cached_results]
        
        # 2. Query IBKR API
        try:
            logger.info(f"Searching IBKR for symbols matching '{query}'")
            ibkr_results = await self.ibkr.search_contracts(query)
            
            if not ibkr_results or not isinstance(ibkr_results, list):
                logger.warning(f"IBKR search returned no results for '{query}'")
                return []
            
            # 3. Cache results and return
            for result in ibkr_results[:limit]:
                symbol = await self._cache_symbol(result)
                if symbol:
                    results.append(symbol.to_dict())
            
            logger.info(f"Symbol search '{query}' returned {len(results)} results from IBKR")
            return results
            
        except Exception as e:
            logger.error(f"Symbol search failed for '{query}': {str(e)}")
            # Fallback to cache even if stale
            cached_results = self._search_cache(query, limit, include_stale=True)
            if cached_results:
                logger.warning(f"Returning {len(cached_results)} stale cached results due to API error")
                return [s.to_dict() for s in cached_results]
            raise
    
    async def get_by_conid(
        self,
        conid: int,
        refresh_if_stale: bool = True
    ) -> Optional[Symbol]:
        """
        Get symbol by IBKR contract ID (conid).
        
        Args:
            conid: IBKR contract ID
            refresh_if_stale: Whether to refresh from IBKR if cache is stale
            
        Returns:
            Symbol object or None if not found
        """
        # Check cache
        symbol = self.db.query(Symbol).filter(Symbol.conid == conid).first()
        
        # If found and fresh, return
        if symbol and not symbol.is_stale(self.cache_ttl_days):
            logger.debug(f"Symbol {conid} found in cache (fresh)")
            return symbol
        
        # If found but stale and refresh disabled, return anyway
        if symbol and not refresh_if_stale:
            logger.debug(f"Symbol {conid} found in cache (stale, but refresh disabled)")
            return symbol
        
        # Need to fetch from IBKR
        try:
            logger.info(f"Fetching contract details for conid {conid} from IBKR")
            contract_info = await self.ibkr.get_contract_details(conid)
            
            if not contract_info:
                logger.warning(f"No contract details found for conid {conid}")
                return symbol  # Return stale data if available
            
            # Update or create
            if symbol:
                symbol = await self._update_symbol(symbol, contract_info)
                logger.info(f"Updated cached symbol {conid}")
            else:
                symbol = await self._cache_symbol(contract_info)
                logger.info(f"Cached new symbol {conid}")
            
            return symbol
            
        except Exception as e:
            logger.error(f"Failed to fetch contract {conid}: {str(e)}")
            return symbol  # Return stale data if available
    
    async def get_by_symbol(
        self,
        symbol_str: str,
        exchange: Optional[str] = None
    ) -> List[Symbol]:
        """
        Get symbols by symbol string.
        
        Args:
            symbol_str: Symbol string (e.g., "AAPL")
            exchange: Optional exchange filter
            
        Returns:
            List of matching Symbol objects
        """
        query = self.db.query(Symbol).filter(Symbol.symbol == symbol_str.upper())
        
        if exchange:
            query = query.filter(Symbol.exchange == exchange)
        
        symbols = query.all()
        logger.debug(f"Found {len(symbols)} symbols matching '{symbol_str}'")
        return symbols
    
    async def batch_cache_symbols(
        self,
        conids: List[int]
    ) -> Dict[int, Symbol]:
        """
        Cache multiple symbols by conid.
        
        Args:
            conids: List of IBKR contract IDs
            
        Returns:
            Dictionary mapping conid to Symbol object
        """
        result = {}
        
        for conid in conids:
            try:
                symbol = await self.get_by_conid(conid, refresh_if_stale=True)
                if symbol:
                    result[conid] = symbol
            except Exception as e:
                logger.error(f"Failed to cache symbol {conid}: {str(e)}")
        
        logger.info(f"Batch cached {len(result)}/{len(conids)} symbols")
        return result
    
    async def refresh_stale_cache(
        self,
        max_symbols: int = 100
    ) -> int:
        """
        Refresh stale cached symbols.
        
        Args:
            max_symbols: Maximum number of symbols to refresh in one call
            
        Returns:
            Number of symbols refreshed
        """
        # Find stale symbols
        cutoff_date = datetime.now() - timedelta(days=self.cache_ttl_days)
        stale_symbols = self.db.query(Symbol).filter(
            Symbol.last_updated < cutoff_date
        ).limit(max_symbols).all()
        
        logger.info(f"Found {len(stale_symbols)} stale symbols to refresh")
        
        refreshed = 0
        for symbol in stale_symbols:
            try:
                await self.get_by_conid(symbol.conid, refresh_if_stale=True)
                refreshed += 1
            except Exception as e:
                logger.error(f"Failed to refresh symbol {symbol.conid}: {str(e)}")
        
        logger.info(f"Refreshed {refreshed}/{len(stale_symbols)} stale symbols")
        return refreshed
    
    def _search_cache(
        self,
        query: str,
        limit: int,
        include_stale: bool = False
    ) -> List[Symbol]:
        """
        Search cached symbols.
        
        Args:
            query: Search query
            limit: Maximum results
            include_stale: Whether to include stale results
            
        Returns:
            List of matching Symbol objects
        """
        query_upper = query.upper()
        
        # Build query
        db_query = self.db.query(Symbol).filter(
            or_(
                Symbol.symbol.ilike(f"%{query_upper}%"),
                Symbol.name.ilike(f"%{query}%")
            )
        )
        
        # Filter out stale if requested
        if not include_stale:
            cutoff_date = datetime.now() - timedelta(days=self.cache_ttl_days)
            db_query = db_query.filter(Symbol.last_updated >= cutoff_date)
        
        results = db_query.limit(limit).all()
        return results
    
    async def _cache_symbol(
        self,
        contract_info: Dict[str, Any]
    ) -> Optional[Symbol]:
        """
        Cache a symbol from IBKR contract info.
        
        Args:
            contract_info: Contract information from IBKR
            
        Returns:
            Created Symbol object or None
        """
        conid = contract_info.get('conid')
        if not conid:
            logger.warning("Contract info missing conid, skipping cache")
            return None
        
        # Check if already exists
        existing = self.db.query(Symbol).filter(Symbol.conid == conid).first()
        if existing:
            return await self._update_symbol(existing, contract_info)
        
        # Create new
        symbol = Symbol(
            conid=conid,
            symbol=contract_info.get('symbol', 'UNKNOWN').upper(),
            name=contract_info.get('companyName') or contract_info.get('name'),
            exchange=contract_info.get('exchange'),
            currency=contract_info.get('currency'),
            asset_type=contract_info.get('assetClass') or contract_info.get('secType'),
            last_updated=datetime.now()
        )
        
        self.db.add(symbol)
        self.db.commit()
        self.db.refresh(symbol)
        
        logger.debug(f"Cached symbol: {symbol.symbol} (conid: {conid})")
        return symbol
    
    async def _update_symbol(
        self,
        symbol: Symbol,
        contract_info: Dict[str, Any]
    ) -> Symbol:
        """
        Update an existing cached symbol.
        
        Args:
            symbol: Existing Symbol object
            contract_info: Updated contract information from IBKR
            
        Returns:
            Updated Symbol object
        """
        symbol.symbol = contract_info.get('symbol', symbol.symbol).upper()
        symbol.name = contract_info.get('companyName') or contract_info.get('name') or symbol.name
        symbol.exchange = contract_info.get('exchange') or symbol.exchange
        symbol.currency = contract_info.get('currency') or symbol.currency
        symbol.asset_type = contract_info.get('assetClass') or contract_info.get('secType') or symbol.asset_type
        symbol.last_updated = datetime.now()
        
        self.db.commit()
        self.db.refresh(symbol)
        
        logger.debug(f"Updated symbol: {symbol.symbol} (conid: {symbol.conid})")
        return symbol


def get_symbol_service(db: Session) -> SymbolService:
    """Factory function for SymbolService."""
    return SymbolService(db)

