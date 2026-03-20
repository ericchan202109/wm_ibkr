"""Market data API endpoints."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from backend.core.database import get_db
from backend.services.ibkr_service import IBKRService
from backend.models.market import MarketData
from typing import List
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market", tags=["market"])


@router.get("/search/{symbol}")
async def search_contracts(symbol: str):
    """Search for contracts by symbol."""
    try:
        ibkr = IBKRService()
        contracts = await ibkr.search_contracts(symbol)
        return {"contracts": contracts}
    except Exception as e:
        logger.error(f"Failed to search contracts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/contract/{conid}")
async def get_contract_details(conid: int):
    """Get contract details by conid."""
    try:
        ibkr = IBKRService()
        details = await ibkr.get_contract_details(conid)
        return details
    except Exception as e:
        logger.error(f"Failed to get contract details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/snapshot/{conid}")
async def get_market_snapshot(conid: int):
    """Get real-time market data snapshot."""
    try:
        ibkr = IBKRService()
        # Request common fields: last price, bid, ask, volume
        fields = ['31', '84', '86', '87']
        snapshot = await ibkr.get_market_data_snapshot([conid], fields)
        return snapshot
    except Exception as e:
        logger.error(f"Failed to get market snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/historical/{conid}")
async def get_historical_data(
    conid: int,
    period: str = "1y",
    bar: str = "1d",
    db: Session = Depends(get_db)
):
    """Get historical market data."""
    try:
        ibkr = IBKRService()
        data = await ibkr.get_historical_data(conid, period, bar)
        
        # Optionally save to database
        # (implement if you want to cache historical data)
        
        return data
    except Exception as e:
        logger.error(f"Failed to get historical data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

