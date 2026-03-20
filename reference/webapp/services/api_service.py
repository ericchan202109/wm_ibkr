import requests
from webapp.config.settings import (
    logger, BASE_API_URL, ACCOUNT_ID
)
from webapp.services.db_service import save_to_nocodb, get_from_nocodb

def is_authenticated():
    """Check if user is authenticated with IBKR API."""
    try:
        r = requests.get(f"{BASE_API_URL}/tickle", verify=False, timeout=15)
        return r.status_code == 200
    except requests.RequestException:
        return False

def fetch_market_data(conid, period, bar, outside_rth, mode='realtime'):
    """Fetch market data from IBKR API or cached database."""
    logger.debug(f"Fetching data for {conid}, {period}, {bar}, {outside_rth}, {mode}")
    if mode == 'offline':
        cached_data = get_from_nocodb(conid, period, bar)
        if cached_data:
            return cached_data

    url = f"{BASE_API_URL}/iserver/marketdata/history"
    params = {'conid': conid, 'period': period, 'bar': bar, 'outsideRth': outside_rth}
    try:
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        if mode == 'realtime':
            save_to_nocodb(conid, period, bar, data)
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching market data: {str(e)}")
        raise

def get_live_market_data(conids, fields=None):
    """Get live market data snapshot for specified contracts.
    
    Args:
        conids: List of contract IDs or comma-separated string
        fields: List of field codes to retrieve (default set if None)
        
    Field codes:
        31: Last price
        84: Bid price
        85: Ask price 
        86: Volume
        88: High price
        7059: Low price
    """
    if fields is None:
        fields = [31, 84, 85, 86, 88, 7059]  # Default fields
    
    # Format conids to comma-separated string if list
    if isinstance(conids, list):
        conids = ','.join(map(str, conids))
    
    # Format fields to comma-separated string if list
    if isinstance(fields, list):
        fields = ','.join(map(str, fields))
        
    url = f"{BASE_API_URL}/iserver/marketdata/snapshot"
    params = {
        'conids': conids,
        'fields': fields
    }
    
    try:
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching live market data: {str(e)}")
        if e.response:
            logger.error(f"Response: {e.response.text}")
        raise

def calculate_adjusted_price(conid, base_price, side, adjustment_percent=None):
    """Calculate adjusted price based on current market data and order side.
    
    Args:
        conid: Contract ID
        base_price: Starting price suggestion
        side: 'BUY' or 'SELL'
        adjustment_percent: Custom adjustment percentage
        
    Returns:
        Adjusted price
    """
    from webapp.config.settings import PRICE_ADJUST_BUY_PERCENT, PRICE_ADJUST_SELL_PERCENT
    
    # If adjustment_percent provided, use it, otherwise use default from config
    if adjustment_percent is None:
        adjustment_percent = PRICE_ADJUST_BUY_PERCENT if side.upper() == 'BUY' else PRICE_ADJUST_SELL_PERCENT
    
    # Get latest market data
    try:
        market_data = get_live_market_data(conid, [31, 84, 85])  # Last, bid, ask
        
        if isinstance(market_data, list) and len(market_data) > 0:
            data = market_data[0]
            
            last_price = data.get('31', None)
            bid_price = data.get('84', None)
            ask_price = data.get('85', None)
            
            # Determine reference price (prefer last price if available)
            reference_price = last_price
            if reference_price is None:
                if side.upper() == 'BUY':
                    reference_price = bid_price  # For buy orders, reference bid price if last not available
                else:
                    reference_price = ask_price  # For sell orders, reference ask price if last not available
            
            # If we have market data, use it to adjust price
            if reference_price is not None:
                # Calculate adjustment factor (convert percentage to decimal)
                adjustment_factor = 1 + (adjustment_percent / 100)
                
                # Apply adjustment
                return round(reference_price * adjustment_factor, 2)
    
        # If any issues with market data, fall back to base price
        return base_price
        
    except Exception as e:
        logger.error(f"Error calculating adjusted price: {str(e)}")
        return base_price  # Fall back to original price on error

def place_order(conid, order_type, price, quantity, side, tif=None, use_price_algo=True, price_adjustment=None):
    """Place an order with improved price management."""
    from webapp.config.settings import DEFAULT_TIF
    
    # Use default TIF if not specified
    if tif is None:
        tif = DEFAULT_TIF
        
    # Apply price management algorithm if requested
    if use_price_algo and order_type.upper() in ['LMT', 'LIMIT']:
        adjusted_price = calculate_adjusted_price(conid, price, side, price_adjustment)
        logger.info(f"Price adjusted from {price} to {adjusted_price} for {side} order")
        price = adjusted_price
    
    data = {
        "orders": [{
            "conid": conid,
            "orderType": order_type,
            "price": price,
            "quantity": quantity,
            "side": side,
            "tif": tif
        }]
    }
    
    logger.info(f"Placing order: {data}")
    
    try:
        r = requests.post(f"{BASE_API_URL}/iserver/account/{ACCOUNT_ID}/orders", json=data, verify=False)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Error placing order: {str(e)}")
        if e.response:
            logger.error(f"Response: {e.response.text}")
        raise

def send_telegram_notification(order_id, conid, status):
    """Send a Telegram notification via n8n webhook."""
    from webapp.config.settings import N8N_WEBHOOK_URL
    from datetime import datetime
    
    try:
        payload = {
            "order_id": order_id,
            "conid": conid,
            "status": status,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=5)
        response.raise_for_status()
        logger.info(f"Sent Telegram notification for order {order_id}")
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram notification for order {order_id}: {str(e)}") 