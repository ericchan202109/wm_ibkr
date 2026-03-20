"""Order management API endpoints."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from backend.core.database import get_db
from backend.services.ibkr_service import IBKRService
# from backend.models.order import Order
from backend.schemas.order import OrderCreate, OrderResponse
from typing import List
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("")
async def list_orders():
    """List all orders from database."""
    # Simple test response
    return [{"id": 1, "test": "order"}]


@router.get("/live")
async def get_live_orders():
    """Get live orders from IBKR."""
    try:
        ibkr = IBKRService()
        orders = await ibkr.get_live_orders()
        return {"orders": orders}
    except Exception as e:
        logger.error(f"Failed to get live orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=OrderResponse, status_code=201)
async def place_order(order_data: OrderCreate, db: Session = Depends(get_db)):
    """Place a new order."""
    try:
        ibkr = IBKRService()
        
        # Place order via IBKR
        result = await ibkr.place_order(
            conid=order_data.conid,
            order_type=order_data.order_type,
            side=order_data.side,
            quantity=order_data.quantity,
            price=order_data.price,
            aux_price=order_data.aux_price,
            tif=order_data.tif
        )
        
        # Save to database
        order = Order(
            strategy_id=order_data.strategy_id,
            decision_id=order_data.decision_id,
            conid=order_data.conid,
            code=order_data.code,
            type=order_data.side.lower(),
            order_type=order_data.order_type,
            quantity=order_data.quantity,
            price=order_data.price or 0,
            status='submitted',
            submitted_at=datetime.now(timezone.utc),
            ibkr_order_id=str(result.get('order_id', ''))
        )
        db.add(order)
        db.commit()
        db.refresh(order)
        
        logger.info(f"Order placed: {order.id}")
        return order
        
    except Exception as e:
        logger.error(f"Failed to place order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{order_id}")
async def cancel_order(order_id: str):
    """Cancel an order."""
    try:
        ibkr = IBKRService()
        result = await ibkr.cancel_order(order_id)
        return result
    except Exception as e:
        logger.error(f"Failed to cancel order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(order_id: int, db: Session = Depends(get_db)):
    """Get order by ID."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
