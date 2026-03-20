"""Tests for OrderManager."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from backend.services.order_manager import OrderManager
from backend.models.order import Order
from backend.models.trading_signal import TradingSignal
from backend.models.strategy import Strategy


class TestOrderManager:
    """Test suite for OrderManager."""
    
    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        db = MagicMock()
        return db
    
    @pytest.fixture
    def order_manager(self, mock_db):
        """Create an OrderManager instance with mock db."""
        return OrderManager(mock_db)
    
    @pytest.fixture
    def sample_signal(self):
        """Create a sample TradingSignal."""
        return TradingSignal(
            id=1,
            symbol="AAPL",
            signal_type="BUY",
            confidence=0.85,
            entry_price_low=150.00,
            entry_price_high=151.00,
            stop_loss=145.00,
            target_conservative=160.00,
            position_size_percent=2.0
        )
    
    @pytest.fixture
    def sample_strategy(self):
        """Create a sample Strategy."""
        return Strategy(
            id=1,
            name="Test Strategy",
            symbol_conid=265598,
            risk_params={"default_quantity": 100}
        )
    
    @pytest.fixture
    def sample_order(self):
        """Create a sample Order."""
        return Order(
            id=1,
            strategy_id=1,
            signal_id=1,
            conid=265598,
            side="BUY",
            quantity=100,
            price=150.50,
            order_type="LMT",
            tif="DAY",
            status="pending"
        )
    
    @pytest.mark.asyncio
    async def test_create_order_from_signal(self, order_manager, mock_db, sample_signal, sample_strategy):
        """Test creating an order from a trading signal."""
        # Execute
        order = await order_manager.create_order_from_signal(
            signal=sample_signal,
            strategy=sample_strategy
        )
        
        # Assert
        assert order is not None
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_submit_order_success(self, order_manager, mock_db, sample_order):
        """Test successfully submitting an order to IBKR."""
        # Mock IBKR response
        ibkr_response = {
            'orderId': 'IBKR123456',
            'status': 'Submitted'
        }
        
        with patch.object(order_manager.ibkr, 'place_order', new_callable=AsyncMock) as mock_place:
            mock_place.return_value = ibkr_response
            
            # Execute
            result = await order_manager.submit_order(sample_order)
            
            # Assert
            assert result.status == "submitted"
            assert result.ibkr_order_id == 'IBKR123456'
            assert result.submitted_at is not None
            mock_place.assert_called_once()
            mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_submit_order_validation_failure(self, order_manager, mock_db, sample_order):
        """Test submitting an order that fails validation."""
        # Make order invalid
        sample_order.quantity = 0
        
        # Execute
        result = await order_manager.submit_order(sample_order)
        
        # Assert
        assert result.status == "rejected"
        assert result.error_message is not None
        mock_db.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_submit_order_dry_run(self, order_manager, mock_db, sample_order):
        """Test dry run mode (validate but don't submit)."""
        # Execute
        result = await order_manager.submit_order(sample_order, dry_run=True)
        
        # Assert
        assert result.status == "validated"
        mock_db.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_order_status(self, order_manager, mock_db, sample_order):
        """Test updating order status from IBKR."""
        # Setup
        sample_order.ibkr_order_id = "IBKR123456"
        
        # Mock IBKR status response
        status_response = {
            'status': 'Filled',
            'filled_quantity': 100,
            'avg_price': 150.75,
            'commission': 1.50
        }
        
        with patch.object(order_manager.ibkr, 'get_order_status', new_callable=AsyncMock) as mock_status:
            mock_status.return_value = status_response
            
            # Execute
            result = await order_manager.update_order_status(sample_order)
            
            # Assert
            assert result.status == "filled"
            assert result.filled_quantity == 100
            assert result.filled_price == 150.75
            assert result.commission == 1.50
            assert result.filled_at is not None
            mock_status.assert_called_once_with("IBKR123456")
            mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_cancel_order(self, order_manager, mock_db, sample_order):
        """Test cancelling an order."""
        # Setup
        sample_order.ibkr_order_id = "IBKR123456"
        sample_order.status = "submitted"
        
        # Mock IBKR cancel response
        cancel_response = {'status': 'Cancelled'}
        
        with patch.object(order_manager.ibkr, 'cancel_order', new_callable=AsyncMock) as mock_cancel:
            mock_cancel.return_value = cancel_response
            
            # Execute
            result = await order_manager.cancel_order(sample_order)
            
            # Assert
            assert result.status == "cancelled"
            mock_cancel.assert_called_once_with("IBKR123456")
            mock_db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_cancel_order_not_submitted(self, order_manager, mock_db, sample_order):
        """Test cancelling an order that hasn't been submitted yet."""
        # Execute
        result = await order_manager.cancel_order(sample_order)
        
        # Assert
        assert result.status == "cancelled"
        mock_db.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_orders(self, order_manager, mock_db, sample_order):
        """Test getting all active orders."""
        # Setup
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [sample_order]
        
        # Execute
        results = await order_manager.get_active_orders()
        
        # Assert
        assert len(results) == 1
        assert results[0].status == "pending"
    
    @pytest.mark.asyncio
    async def test_monitor_active_orders(self, order_manager, mock_db, sample_order):
        """Test monitoring all active orders."""
        # Setup
        sample_order.ibkr_order_id = "IBKR123456"
        
        with patch.object(order_manager, 'get_active_orders', return_value=[sample_order]):
            with patch.object(order_manager, 'update_order_status', new_callable=AsyncMock) as mock_update:
                mock_update.return_value = sample_order
                
                # Execute
                result = await order_manager.monitor_active_orders()
                
                # Assert
                assert result['total_active'] == 1
                assert result['updated'] == 1
                assert len(result['errors']) == 0
                mock_update.assert_called_once()
    
    def test_validate_order_success(self, order_manager, sample_order):
        """Test order validation with valid order."""
        # Execute
        result = order_manager._validate_order(sample_order)
        
        # Assert
        assert result['valid'] is True
        assert len(result['errors']) == 0
    
    def test_validate_order_missing_fields(self, order_manager, sample_order):
        """Test order validation with missing fields."""
        # Make order invalid
        sample_order.conid = None
        
        # Execute
        result = order_manager._validate_order(sample_order)
        
        # Assert
        assert result['valid'] is False
        assert len(result['errors']) > 0
        assert any('conid' in err.lower() for err in result['errors'])
    
    def test_validate_order_invalid_side(self, order_manager, sample_order):
        """Test order validation with invalid side."""
        # Make order invalid
        sample_order.side = "INVALID"
        
        # Execute
        result = order_manager._validate_order(sample_order)
        
        # Assert
        assert result['valid'] is False
        assert any('side' in err.lower() for err in result['errors'])
    
    def test_map_ibkr_status(self, order_manager):
        """Test mapping IBKR status to internal status."""
        # Test various status mappings
        assert order_manager._map_ibkr_status("Submitted") == "submitted"
        assert order_manager._map_ibkr_status("Filled") == "filled"
        assert order_manager._map_ibkr_status("PartiallyFilled") == "partially_filled"
        assert order_manager._map_ibkr_status("Cancelled") == "cancelled"
        assert order_manager._map_ibkr_status("Rejected") == "rejected"
        assert order_manager._map_ibkr_status("UnknownStatus") == "unknownstatus"
    
    def test_calculate_position_size(self, order_manager, sample_signal, sample_strategy):
        """Test position size calculation."""
        # Execute
        size = order_manager._calculate_position_size(sample_signal, sample_strategy)
        
        # Assert
        assert size == 100  # Default from risk_params
        assert size > 0

