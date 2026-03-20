"""Tests for LineageTracker."""
import pytest
from unittest.mock import MagicMock
from datetime import datetime
from backend.services.lineage_tracker import LineageTracker
from backend.models.lineage import LineageRecord


class TestLineageTracker:
    """Test suite for LineageTracker."""
    
    @pytest.fixture
    def mock_db(self):
        """Create a mock database session."""
        db = MagicMock()
        return db
    
    @pytest.fixture
    def lineage_tracker(self, mock_db):
        """Create a LineageTracker instance with mock db."""
        return LineageTracker(mock_db)
    
    @pytest.fixture
    def sample_lineage(self):
        """Create a sample LineageRecord."""
        return LineageRecord(
            id=1,
            execution_id="exec_123",
            step_name="fetch_market_data",
            step_number=2,
            input_data={"conid": 265598},
            output_data={"data_points": 100},
            metadata={"strategy_id": 1},
            status="success",
            duration_ms=500,
            recorded_at=datetime.now()
        )
    
    @pytest.mark.asyncio
    async def test_record_step(self, lineage_tracker, mock_db):
        """Test recording a lineage step."""
        # Execute
        await lineage_tracker.record_step(
            execution_id="exec_123",
            step_name="fetch_market_data",
            step_number=2,
            input_data={"conid": 265598},
            output_data={"data_points": 100},
            metadata={"strategy_id": 1},
            duration_ms=500,
            db=mock_db
        )
        
        # Assert
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_record_step_with_error(self, lineage_tracker, mock_db):
        """Test recording a lineage step with an error."""
        # Execute
        await lineage_tracker.record_step(
            execution_id="exec_123",
            step_name="fetch_market_data",
            step_number=2,
            input_data={"conid": 265598},
            output_data={},
            error="API timeout",
            duration_ms=5000,
            db=mock_db
        )
        
        # Assert
        mock_db.add.assert_called_once()
        # Check that the record was added
        call_args = mock_db.add.call_args[0][0]
        assert call_args.error == "API timeout"
        assert call_args.status == "error"
    
    @pytest.mark.asyncio
    async def test_get_execution_lineage(self, lineage_tracker, mock_db, sample_lineage):
        """Test getting complete lineage for an execution."""
        # Setup
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [sample_lineage]
        
        # Execute
        results = await lineage_tracker.get_execution_lineage("exec_123", db=mock_db)
        
        # Assert
        assert len(results) == 1
        assert results[0].execution_id == "exec_123"
        assert results[0].step_name == "fetch_market_data"
    
    @pytest.mark.asyncio
    async def test_get_step_lineage(self, lineage_tracker, mock_db, sample_lineage):
        """Test getting lineage for a specific step."""
        # Setup
        mock_db.query.return_value.filter.return_value.first.return_value = sample_lineage
        
        # Execute
        result = await lineage_tracker.get_step_lineage("exec_123", "fetch_market_data", db=mock_db)
        
        # Assert
        assert result is not None
        assert result.step_name == "fetch_market_data"
    
    def test_lineage_record_to_dict(self, sample_lineage):
        """Test converting LineageRecord to dictionary."""
        # Execute
        result = sample_lineage.to_dict()
        
        # Assert
        assert result['id'] == 1
        assert result['execution_id'] == "exec_123"
        assert result['step_name'] == "fetch_market_data"
        assert result['status'] == "success"
        assert result['duration_ms'] == 500
        assert 'recorded_at' in result

