"""
Order Manager Service - Handles IBKR order placement and tracking.

This service manages the complete order lifecycle:
1. Create orders from trading signals
2. Submit orders to IBKR
3. Monitor order status
4. Update order fills and status
5. Cancel orders if needed
6. Record order history
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from backend.models.order import Order
from backend.models.trading_signal import TradingSignal
from backend.models.strategy import Strategy
from backend.services.ibkr_service import IBKRService

logger = logging.getLogger(__name__)


class OrderManager:
    """
    Manages IBKR order placement, tracking, and lifecycle.
    
    Features:
    - Order creation from signals
    - IBKR order submission
    - Status monitoring
    - Fill tracking
    - Order cancellation
    - History queries
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.ibkr = IBKRService()
    
    async def create_order_from_signal(
        self,
        signal: TradingSignal,
        strategy: Strategy,
        order_type: str = "LMT",
        tif: str = "DAY",
        quantity: Optional[int] = None,
        price: Optional[float] = None
    ) -> Order:
        """
        Create an order from a trading signal.
        
        Args:
            signal: TradingSignal to create order from
            strategy: Parent strategy
            order_type: Order type (LMT, MKT, STP, STPLMT)
            tif: Time in force (DAY, GTC, IOC)
            quantity: Order quantity (calculated from risk if None)
            price: Limit price (from signal if None)
            
        Returns:
            Created Order object
        """
        try:
            # Determine quantity if not provided
            if quantity is None:
                quantity = self._calculate_position_size(signal, strategy)
            
            # Determine price if not provided
            if price is None and order_type in ["LMT", "STPLMT"]:
                # Use entry price from signal
                if signal.signal_type == "BUY":
                    price = signal.entry_price_low or signal.entry_price_high
                else:
                    price = signal.entry_price_high or signal.entry_price_low
            
            # Determine side
            side = signal.signal_type  # BUY or SELL
            
            # Get conid from strategy symbol
            conid = strategy.symbol_conid
            if not conid:
                raise ValueError(f"Strategy {strategy.id} has no symbol configured")
            
            # Create order
            order = Order(
                strategy_id=strategy.id,
                signal_id=signal.id,
                conid=conid,
                side=side,
                quantity=quantity,
                price=price,
                order_type=order_type,
                tif=tif,
                status="pending"
            )
            
            self.db.add(order)
            self.db.commit()
            self.db.refresh(order)
            
            logger.info(
                f"Created order: {side} {quantity} shares @ {price} "
                f"for signal {signal.id} (order ID: {order.id})"
            )
            
            return order
            
        except Exception as e:
            logger.error(f"Failed to create order from signal {signal.id}: {str(e)}")
            raise
    
    async def submit_order(
        self,
        order: Order,
        dry_run: bool = False
    ) -> Order:
        """
        Submit an order to IBKR.
        
        Args:
            order: Order object to submit
            dry_run: If True, validate but don't actually submit
            
        Returns:
            Updated Order object with IBKR response
        """
        try:
            # Validate order
            validation = self._validate_order(order)
            if not validation['valid']:
                order.status = "rejected"
                order.error_message = f"Validation failed: {validation['errors']}"
                self.db.commit()
                logger.error(f"Order {order.id} validation failed: {validation['errors']}")
                return order
            
            if dry_run:
                logger.info(f"Dry run: Would submit order {order.id}")
                order.status = "validated"
                self.db.commit()
                return order
            
            # Submit to IBKR
            logger.info(f"Submitting order {order.id} to IBKR: {order.side} {order.quantity} @ {order.price}")
            
            ibkr_response = await self.ibkr.place_order(
                conid=order.conid,
                side=order.side,
                quantity=order.quantity,
                order_type=order.order_type,
                price=order.price,
                tif=order.tif
            )
            
            # Update order with IBKR response
            order.ibkr_order_id = ibkr_response.get('orderId') or ibkr_response.get('order_id')
            order.ibkr_response = ibkr_response
            order.status = "submitted"
            order.submitted_at = datetime.now()
            
            if 'error' in ibkr_response or 'message' in ibkr_response:
                order.status = "rejected"
                order.error_message = ibkr_response.get('error') or ibkr_response.get('message')
                logger.error(f"Order {order.id} rejected by IBKR: {order.error_message}")
            else:
                logger.info(f"Order {order.id} submitted successfully. IBKR order ID: {order.ibkr_order_id}")
            
            self.db.commit()
            self.db.refresh(order)
            
            return order
            
        except Exception as e:
            order.status = "error"
            order.error_message = str(e)
            self.db.commit()
            logger.error(f"Failed to submit order {order.id}: {str(e)}", exc_info=True)
            raise
    
    async def update_order_status(
        self,
        order: Order,
        ibkr_order_id: Optional[str] = None
    ) -> Order:
        """
        Update order status from IBKR.
        
        Args:
            order: Order object to update
            ibkr_order_id: IBKR order ID (uses order.ibkr_order_id if None)
            
        Returns:
            Updated Order object
        """
        try:
            ibkr_id = ibkr_order_id or order.ibkr_order_id
            if not ibkr_id:
                logger.warning(f"Order {order.id} has no IBKR order ID, cannot update status")
                return order
            
            # Get order status from IBKR
            status_response = await self.ibkr.get_order_status(ibkr_id)
            
            if not status_response:
                logger.warning(f"No status response for order {order.id} (IBKR ID: {ibkr_id})")
                return order
            
            # Update order fields
            old_status = order.status
            order.status = self._map_ibkr_status(status_response.get('status', ''))
            order.filled_quantity = status_response.get('filled_quantity', 0) or status_response.get('filledQuantity', 0)
            order.remaining_quantity = status_response.get('remaining_quantity') or status_response.get('remainingQuantity')
            order.filled_price = status_response.get('avg_price') or status_response.get('avgPrice')
            order.commission = status_response.get('commission')
            
            # Update timestamps
            if order.status == "filled" and old_status != "filled":
                order.filled_at = datetime.now()
            
            order.updated_at = datetime.now()
            
            self.db.commit()
            self.db.refresh(order)
            
            if old_status != order.status:
                logger.info(f"Order {order.id} status changed: {old_status} → {order.status}")
            
            return order
            
        except Exception as e:
            logger.error(f"Failed to update order {order.id} status: {str(e)}")
            raise
    
    async def cancel_order(
        self,
        order: Order
    ) -> Order:
        """
        Cancel an order.
        
        Args:
            order: Order object to cancel
            
        Returns:
            Updated Order object
        """
        try:
            if not order.is_active():
                logger.warning(f"Order {order.id} is already in terminal state: {order.status}")
                return order
            
            if not order.ibkr_order_id:
                # Order not yet submitted, just mark as cancelled
                order.status = "cancelled"
                self.db.commit()
                logger.info(f"Order {order.id} cancelled (not yet submitted)")
                return order
            
            # Cancel via IBKR
            logger.info(f"Cancelling order {order.id} (IBKR ID: {order.ibkr_order_id})")
            cancel_response = await self.ibkr.cancel_order(order.ibkr_order_id)
            
            order.status = "cancelled"
            order.updated_at = datetime.now()
            
            if 'error' in cancel_response:
                order.error_message = cancel_response['error']
                logger.error(f"Failed to cancel order {order.id}: {cancel_response['error']}")
            else:
                logger.info(f"Order {order.id} cancelled successfully")
            
            self.db.commit()
            self.db.refresh(order)
            
            return order
            
        except Exception as e:
            logger.error(f"Failed to cancel order {order.id}: {str(e)}")
            raise
    
    async def get_order(
        self,
        order_id: int
    ) -> Optional[Order]:
        """Get order by ID."""
        return self.db.query(Order).filter(Order.id == order_id).first()
    
    async def get_orders_by_strategy(
        self,
        strategy_id: int,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Order]:
        """
        Get orders for a strategy.
        
        Args:
            strategy_id: Strategy ID
            status: Filter by status (optional)
            limit: Maximum results
            
        Returns:
            List of Order objects
        """
        query = self.db.query(Order).filter(Order.strategy_id == strategy_id)
        
        if status:
            query = query.filter(Order.status == status)
        
        orders = query.order_by(Order.created_at.desc()).limit(limit).all()
        return orders
    
    async def get_orders_by_signal(
        self,
        signal_id: int
    ) -> List[Order]:
        """Get orders for a trading signal."""
        return self.db.query(Order).filter(Order.signal_id == signal_id).all()
    
    async def get_active_orders(
        self,
        strategy_id: Optional[int] = None
    ) -> List[Order]:
        """
        Get all active orders (not in terminal state).
        
        Args:
            strategy_id: Filter by strategy (optional)
            
        Returns:
            List of active Order objects
        """
        terminal_statuses = ["filled", "cancelled", "rejected", "inactive"]
        query = self.db.query(Order).filter(~Order.status.in_(terminal_statuses))
        
        if strategy_id:
            query = query.filter(Order.strategy_id == strategy_id)
        
        orders = query.order_by(Order.created_at.desc()).all()
        return orders
    
    async def monitor_active_orders(
        self
    ) -> Dict[str, Any]:
        """
        Monitor all active orders and update their status.
        
        This should be called periodically (e.g., every minute) to
        sync order status with IBKR.
        
        Returns:
            Dictionary with monitoring results
        """
        active_orders = await self.get_active_orders()
        
        updated = 0
        errors = []
        
        for order in active_orders:
            try:
                await self.update_order_status(order)
                updated += 1
            except Exception as e:
                errors.append({
                    "order_id": order.id,
                    "error": str(e)
                })
        
        logger.info(f"Monitored {len(active_orders)} active orders. Updated: {updated}, Errors: {len(errors)}")
        
        return {
            "total_active": len(active_orders),
            "updated": updated,
            "errors": errors
        }
    
    def _calculate_position_size(
        self,
        signal: TradingSignal,
        strategy: Strategy
    ) -> int:
        """
        Calculate position size based on risk parameters.
        
        Args:
            signal: Trading signal
            strategy: Parent strategy
            
        Returns:
            Number of shares to order
        """
        # Get risk parameters from strategy
        risk_params = strategy.risk_params or {}
        
        # Default position size
        position_size_pct = signal.position_size_percent or risk_params.get('position_size_pct', 2.0)
        
        # Calculate based on 2% risk rule
        # TODO: Implement proper position sizing with account balance
        # For now, return a default value
        default_quantity = risk_params.get('default_quantity', 100)
        
        logger.debug(f"Calculated position size: {default_quantity} shares (default)")
        return default_quantity
    
    def _validate_order(
        self,
        order: Order
    ) -> Dict[str, Any]:
        """
        Validate order parameters.
        
        Args:
            order: Order to validate
            
        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []
        
        # Check required fields
        if not order.conid:
            errors.append("Missing conid")
        
        if not order.side:
            errors.append("Missing side")
        elif order.side not in ["BUY", "SELL", "SELL_SHORT"]:
            errors.append(f"Invalid side: {order.side}")
        
        if not order.quantity or order.quantity <= 0:
            errors.append(f"Invalid quantity: {order.quantity}")
        
        if order.order_type not in ["LMT", "MKT", "STP", "STPLMT"]:
            errors.append(f"Invalid order type: {order.order_type}")
        
        if order.order_type in ["LMT", "STPLMT"] and not order.price:
            errors.append(f"Limit price required for {order.order_type} orders")
        
        if order.tif not in ["DAY", "GTC", "IOC"]:
            errors.append(f"Invalid TIF: {order.tif}")
        
        # Warnings
        if order.quantity > 10000:
            warnings.append(f"Large order quantity: {order.quantity}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def _map_ibkr_status(
        self,
        ibkr_status: str
    ) -> str:
        """
        Map IBKR order status to our internal status.
        
        Args:
            ibkr_status: Status from IBKR
            
        Returns:
            Internal status string
        """
        status_map = {
            "Submitted": "submitted",
            "PreSubmitted": "pre_submitted",
            "Filled": "filled",
            "PartiallyFilled": "partially_filled",
            "Cancelled": "cancelled",
            "Rejected": "rejected",
            "Inactive": "inactive",
            "PendingSubmit": "pending",
            "PendingCancel": "cancelling",
        }
        
        return status_map.get(ibkr_status, ibkr_status.lower())


def get_order_manager(db: Session) -> OrderManager:
    """Factory function for OrderManager."""
    return OrderManager(db)
