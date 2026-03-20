"""Tests for PositionManager."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from backend.services.position_manager import PositionManager
from backend.models.position import Position
from backend.models.order import Order


class TestPositionManager:
    """Test suite for PositionManager."""
    
    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None
        return db
    
    @pytest.fixture
    def position_manager(self, mock_db):
        """Create a PositionManager instance with mock db."""
        return PositionManager(mock_db)
    
    @pytest.fixture
    def sample_buy_order(self):
        """Create a sample buy order."""
        return Order(
            id=1,
            strategy_id=1,
            conid=265598,
            side='BUY',
            quantity=100,
            filled_quantity=100,
            price=150.00,
            filled_price=150.50,
            order_type='LMT',
            status='filled',
            ibkr_order_id='IBKR123'
        )
    
    @pytest.fixture
    def sample_sell_order(self):
        """Create a sample sell order."""
        return Order(
            id=2,
            strategy_id=1,
            conid=265598,
            side='SELL',
            quantity=50,
            filled_quantity=50,
            price=160.00,
            filled_price=160.25,
            order_type='LMT',
            status='filled',
            ibkr_order_id='IBKR124'
        )
    
    @pytest.fixture
    def sample_position(self):
        """Create a sample position."""
        return Position(
            id=1,
            conid=265598,
            strategy_id=1,
            quantity=100,
            average_price=150.00,
            last_price=155.00,
            unrealized_pnl=500.00,
            realized_pnl=0.00,
            is_closed=False
        )
    
    @pytest.mark.asyncio
    async def test_update_from_buy_fill_new_position(
        self,
        position_manager,
        mock_db,
        sample_buy_order
    ):
        """Test creating new position from buy order fill."""
        # No existing position
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        position = await position_manager.update_from_fill(sample_buy_order)
        
        assert position is not None
        assert position.quantity == 100
        assert position.average_price == 150.50
        assert position.conid == 265598
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_from_buy_fill_add_to_position(
        self,
        position_manager,
        mock_db,
        sample_buy_order,
        sample_position
    ):
        """Test adding to existing position from buy order fill."""
        # Existing position
        mock_db.query.return_value.filter.return_value.first.return_value = sample_position
        
        position = await position_manager.update_from_fill(sample_buy_order)
        
        # Should average the prices
        # (150.00 * 100 + 150.50 * 100) / 200 = 150.25
        assert position.quantity == 200
        assert position.average_price == 150.25
        mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_from_sell_fill_partial(
        self,
        position_manager,
        mock_db,
        sample_sell_order,
        sample_position
    ):
        """Test partial position close from sell order fill."""
        # Existing position with 100 shares
        mock_db.query.return_value.filter.return_value.first.return_value = sample_position
        
        position = await position_manager.update_from_fill(sample_sell_order)
        
        # Should reduce position
        assert position.quantity == 50  # 100 - 50
        assert not position.is_closed
        
        # Should calculate realized P&L
        # (160.25 - 150.00) * 50 = 512.50
        assert position.realized_pnl > 0
        mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_from_sell_fill_complete(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test complete position close from sell order fill."""
        # Existing position
        mock_db.query.return_value.filter.return_value.first.return_value = sample_position
        
        # Sell entire position
        sell_all_order = Order(
            id=3,
            strategy_id=1,
            conid=265598,
            side='SELL',
            quantity=100,
            filled_quantity=100,
            price=160.00,
            filled_price=160.00,
            order_type='MKT',
            status='filled'
        )
        
        position = await position_manager.update_from_fill(sell_all_order)
        
        # Should close position
        assert position.quantity == 0
        assert position.is_closed
        assert position.closed_at is not None
        mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_from_sell_fill_no_position(
        self,
        position_manager,
        mock_db,
        sample_sell_order
    ):
        """Test sell order when no position exists."""
        # No existing position
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        position = await position_manager.update_from_fill(sample_sell_order)
        
        # Should return None and log warning
        assert position is None
    
    @pytest.mark.asyncio
    async def test_get_all_positions(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test getting all positions."""
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
            sample_position
        ]
        
        positions = await position_manager.get_all_positions()
        
        assert len(positions) == 1
        assert positions[0].conid == 265598
    
    @pytest.mark.asyncio
    async def test_get_all_positions_by_strategy(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test getting positions filtered by strategy."""
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
            sample_position
        ]
        
        positions = await position_manager.get_all_positions(strategy_id=1)
        
        assert len(positions) == 1
    
    @pytest.mark.asyncio
    async def test_get_all_positions_include_closed(
        self,
        position_manager,
        mock_db
    ):
        """Test getting positions including closed ones."""
        closed_position = Position(
            id=2,
            conid=265599,
            strategy_id=1,
            quantity=0,
            average_price=100.00,
            is_closed=True
        )
        
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
            closed_position
        ]
        
        positions = await position_manager.get_all_positions(include_closed=True)
        
        assert len(positions) == 1
        assert positions[0].is_closed
    
    @pytest.mark.asyncio
    async def test_get_position(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test getting a specific position by conid."""
        mock_db.query.return_value.filter.return_value.first.return_value = sample_position
        
        position = await position_manager.get_position(265598)
        
        assert position is not None
        assert position.conid == 265598
    
    @pytest.mark.asyncio
    async def test_calculate_portfolio_value(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test calculating portfolio value."""
        # Mock positions
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
            sample_position
        ]
        
        # Mock realized P&L query
        mock_db.query.return_value.filter.return_value.scalar.return_value = 100.00
        
        with patch.object(position_manager, '_update_position_pnl', new_callable=AsyncMock):
            portfolio = await position_manager.calculate_portfolio_value()
            
            assert 'portfolio_value' in portfolio
            assert 'total_cost' in portfolio
            assert 'realized_pnl' in portfolio
            assert 'unrealized_pnl' in portfolio
            assert 'total_pnl' in portfolio
            assert 'return_percent' in portfolio
            assert 'position_count' in portfolio
            
            # Portfolio value = last_price * quantity = 155 * 100 = 15,500
            assert portfolio['portfolio_value'] == 15500
            
            # Total cost = avg_price * quantity = 150 * 100 = 15,000
            assert portfolio['total_cost'] == 15000
    
    @pytest.mark.asyncio
    async def test_sync_with_ibkr(
        self,
        position_manager,
        mock_db
    ):
        """Test syncing positions with IBKR."""
        # Mock IBKR positions
        ibkr_positions = [
            {
                'conid': 265598,
                'quantity': 100,
                'avg_price': 150.00,
                'market_price': 155.00
            }
        ]
        
        with patch.object(position_manager.ibkr, 'get_positions', new_callable=AsyncMock) as mock_ibkr:
            mock_ibkr.return_value = ibkr_positions
            
            # No existing position
            mock_db.query.return_value.filter.return_value.first.return_value = None
            
            result = await position_manager.sync_with_ibkr()
            
            assert result['success']
            assert result['synced'] == 1
            assert result['created'] == 1
            mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_sync_with_ibkr_update_existing(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test syncing updates existing positions."""
        ibkr_positions = [
            {
                'conid': 265598,
                'quantity': 150,  # Updated quantity
                'avg_price': 152.00,  # Updated avg price
                'market_price': 160.00
            }
        ]
        
        with patch.object(position_manager.ibkr, 'get_positions', new_callable=AsyncMock) as mock_ibkr:
            mock_ibkr.return_value = ibkr_positions
            
            # Existing position
            mock_db.query.return_value.filter.return_value.first.return_value = sample_position
            
            result = await position_manager.sync_with_ibkr()
            
            assert result['success']
            assert result['updated'] == 1
    
    @pytest.mark.asyncio
    async def test_get_position_risk_metrics(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test calculating risk metrics for a position."""
        # Mock portfolio value calculation
        portfolio = {
            'portfolio_value': 50000
        }
        
        with patch.object(position_manager, 'calculate_portfolio_value', new_callable=AsyncMock) as mock_portfolio:
            with patch.object(position_manager, '_update_position_pnl', new_callable=AsyncMock):
                mock_portfolio.return_value = portfolio
                
                metrics = await position_manager.get_position_risk_metrics(sample_position)
                
                assert 'position_value' in metrics
                assert 'position_percent' in metrics
                assert 'pnl_percent' in metrics
                assert 'unrealized_pnl' in metrics
                assert 'realized_pnl' in metrics
                assert 'entry_price' in metrics
                assert 'current_price' in metrics
                assert 'quantity' in metrics
                
                # Position value = last_price * quantity = 155 * 100 = 15,500
                assert metrics['position_value'] == 15500
                
                # Position percent = 15,500 / 50,000 * 100 = 31%
                assert metrics['position_percent'] == 31.0
    
    @pytest.mark.asyncio
    async def test_update_position_pnl(
        self,
        position_manager,
        mock_db,
        sample_position
    ):
        """Test updating position P&L with current price."""
        with patch.object(position_manager, '_get_current_price', new_callable=AsyncMock) as mock_price:
            mock_price.return_value = 160.00
            
            await position_manager._update_position_pnl(sample_position)
            
            # Should update last_price
            assert sample_position.last_price == 160.00
            
            # Should calculate unrealized P&L
            # (160 - 150) * 100 = 1000
            assert sample_position.unrealized_pnl == 1000.00
            
            mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_position_pnl_closed_position(
        self,
        position_manager,
        mock_db
    ):
        """Test P&L update for closed position."""
        closed_position = Position(
            id=2,
            conid=265599,
            quantity=0,
            average_price=100.00,
            last_price=110.00,
            is_closed=True
        )
        
        with patch.object(position_manager, '_get_current_price', new_callable=AsyncMock) as mock_price:
            mock_price.return_value = 115.00
            
            await position_manager._update_position_pnl(closed_position)
            
            # Closed position should have 0 unrealized P&L
            assert closed_position.unrealized_pnl == 0
    
    @pytest.mark.asyncio
    async def test_get_current_price(
        self,
        position_manager
    ):
        """Test getting current price from IBKR."""
        with patch.object(position_manager.ibkr, 'get_market_data', new_callable=AsyncMock) as mock_data:
            mock_data.return_value = {
                'last_price': 155.50
            }
            
            price = await position_manager._get_current_price(265598)
            
            assert price == 155.50
            mock_data.assert_called_once_with(265598)
    
    @pytest.mark.asyncio
    async def test_get_current_price_error(
        self,
        position_manager
    ):
        """Test handling error when getting current price."""
        with patch.object(position_manager.ibkr, 'get_market_data', new_callable=AsyncMock) as mock_data:
            mock_data.side_effect = Exception('API error')
            
            price = await position_manager._get_current_price(265598)
            
            assert price is None

