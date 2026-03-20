"""Tests for SymbolService."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from backend.services.symbol_service import SymbolService
from backend.models.symbol import Symbol


class TestSymbolService:
    """Test suite for SymbolService."""
    
    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        db = MagicMock()
        return db
    
    @pytest.fixture
    def symbol_service(self, mock_db):
        """Create a SymbolService instance with mock db."""
        return SymbolService(mock_db)
    
    @pytest.fixture
    def sample_symbol(self):
        """Create a sample Symbol object."""
        return Symbol(
            id=1,
            conid=265598,
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            currency="USD",
            asset_type="STK",
            last_updated=datetime.now()
        )
    
    @pytest.mark.asyncio
    async def test_search_symbols_from_cache(self, symbol_service, mock_db, sample_symbol):
        """Test searching symbols with cache hit."""
        # Setup
        mock_db.query.return_value.filter.return_value.limit.return_value.all.return_value = [sample_symbol]
        
        # Execute
        results = await symbol_service.search_symbols("AAPL", limit=10, use_cache=True)
        
        # Assert
        assert len(results) == 1
        assert results[0]['symbol'] == "AAPL"
        assert results[0]['conid'] == 265598
    
    @pytest.mark.asyncio
    async def test_search_symbols_from_ibkr(self, symbol_service, mock_db):
        """Test searching symbols with cache miss (IBKR fallback)."""
        # Setup - empty cache
        mock_db.query.return_value.filter.return_value.limit.return_value.all.return_value = []
        
        # Mock IBKR response
        ibkr_response = [
            {
                'conid': 265598,
                'symbol': 'AAPL',
                'companyName': 'Apple Inc.',
                'exchange': 'NASDAQ'
            }
        ]
        
        with patch.object(symbol_service.ibkr, 'search_contracts', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = ibkr_response
            
            # Execute
            results = await symbol_service.search_symbols("AAPL", limit=10, use_cache=False)
            
            # Assert
            assert len(results) == 1
            assert results[0]['symbol'] == "AAPL"
            mock_search.assert_called_once_with("AAPL")
    
    @pytest.mark.asyncio
    async def test_get_by_conid_fresh_cache(self, symbol_service, mock_db, sample_symbol):
        """Test getting symbol by conid with fresh cache."""
        # Setup
        mock_db.query.return_value.filter.return_value.first.return_value = sample_symbol
        
        # Execute
        result = await symbol_service.get_by_conid(265598, refresh_if_stale=True)
        
        # Assert
        assert result is not None
        assert result.conid == 265598
        assert result.symbol == "AAPL"
    
    @pytest.mark.asyncio
    async def test_get_by_conid_stale_cache(self, symbol_service, mock_db):
        """Test getting symbol by conid with stale cache triggers refresh."""
        # Setup - stale symbol
        stale_symbol = Symbol(
            id=1,
            conid=265598,
            symbol="AAPL",
            last_updated=datetime.now() - timedelta(days=10)
        )
        mock_db.query.return_value.filter.return_value.first.return_value = stale_symbol
        
        # Mock IBKR response
        with patch.object(symbol_service.ibkr, 'get_contract_details', new_callable=AsyncMock) as mock_details:
            mock_details.return_value = {
                'conid': 265598,
                'symbol': 'AAPL',
                'companyName': 'Apple Inc.'
            }
            
            # Execute
            result = await symbol_service.get_by_conid(265598, refresh_if_stale=True)
            
            # Assert
            mock_details.assert_called_once_with(265598)
    
    @pytest.mark.asyncio
    async def test_batch_cache_symbols(self, symbol_service, mock_db):
        """Test batch caching multiple symbols."""
        conids = [265598, 272093]
        
        # Mock get_by_conid to return symbols
        symbols = {
            265598: Symbol(conid=265598, symbol="AAPL"),
            272093: Symbol(conid=272093, symbol="TSLA")
        }
        
        async def mock_get_by_conid(conid, refresh_if_stale):
            return symbols.get(conid)
        
        with patch.object(symbol_service, 'get_by_conid', side_effect=mock_get_by_conid):
            # Execute
            result = await symbol_service.batch_cache_symbols(conids)
            
            # Assert
            assert len(result) == 2
            assert 265598 in result
            assert 272093 in result
    
    @pytest.mark.asyncio
    async def test_refresh_stale_cache(self, symbol_service, mock_db):
        """Test refreshing stale cached symbols."""
        # Setup - stale symbols
        stale_symbols = [
            Symbol(conid=265598, symbol="AAPL", last_updated=datetime.now() - timedelta(days=10)),
            Symbol(conid=272093, symbol="TSLA", last_updated=datetime.now() - timedelta(days=10))
        ]
        mock_db.query.return_value.filter.return_value.limit.return_value.all.return_value = stale_symbols
        
        # Mock get_by_conid
        with patch.object(symbol_service, 'get_by_conid', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Symbol(conid=265598, symbol="AAPL")
            
            # Execute
            refreshed = await symbol_service.refresh_stale_cache(max_symbols=10)
            
            # Assert
            assert refreshed == 2
            assert mock_get.call_count == 2
    
    def test_symbol_is_stale(self, sample_symbol):
        """Test checking if symbol is stale."""
        # Fresh symbol
        assert not sample_symbol.is_stale(7)
        
        # Make it stale
        sample_symbol.last_updated = datetime.now() - timedelta(days=10)
        assert sample_symbol.is_stale(7)
        
        # No last_updated
        sample_symbol.last_updated = None
        assert sample_symbol.is_stale(7)

