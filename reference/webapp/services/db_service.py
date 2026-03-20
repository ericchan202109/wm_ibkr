import requests
import json
from datetime import datetime
from functools import lru_cache
from webapp.config.settings import (
    logger, NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_TABLE_ID,
    NOCODB_ORDERS_TABLE_ID, NOCODB_TRADES_TABLE_ID
)

def save_to_nocodb(conid, period, bar, data):
    """Save market data to NocoDB."""
    if not all([NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_TABLE_ID]):
        logger.error(
            "NocoDB configuration is incomplete. Check environment variables: NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_TABLE_ID")
        raise ValueError("NocoDB configuration is incomplete")

    url = f"{NOCODB_BASE_URL}/api/v2/tables/{NOCODB_TABLE_ID}/records"
    headers = {
        "xc-token": NOCODB_API_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "conid": str(conid),  # Ensure conid is a string
        "period": period,
        "bar": bar,
        "data": json.dumps(data),  # Store as JSON string
        "created_at": datetime.utcnow().isoformat() + "Z"  # ISO 8601 with UTC marker
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.debug(f"Successfully saved to NocoDB: {conid}, {period}, {bar}")
    except requests.RequestException as e:
        error_msg = f"Error saving to NocoDB: {str(e)}"
        if e.response:
            error_msg += f" - Response: {e.response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)


@lru_cache(maxsize=128)
def get_from_nocodb(conid, period, bar):
    """Get market data from NocoDB with caching."""
    logger.debug(f"Getting data from NocoDB: {conid}, {period}, {bar}")
    if not all([NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_TABLE_ID]):
        logger.error(
            "NocoDB configuration is incomplete. Check environment variables: NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_TABLE_ID")
        return None

    url = f"{NOCODB_BASE_URL}/api/v2/tables/{NOCODB_TABLE_ID}/records"
    headers = {"xc-token": NOCODB_API_TOKEN}
    params = {
        "where": f"(conid,eq,{str(conid)})~and(period,eq,{period})~and(bar,eq,{bar})"
    }

    logger.debug(f"Requesting: {url} with params {params} and headers {headers}")
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        # NocoDB v2 returns records in a 'list' key
        if "list" in data and isinstance(data["list"], list) and len(data["list"]) > 0:
            logger.debug(f"Retrieved data from NocoDB for {conid}, {period}, {bar}")
            return json.loads(data["list"][0]["data"])  # Parse JSON string back to dict
        logger.debug(f"No data found in NocoDB for {conid}, {period}, {bar}")
        return None
    except requests.RequestException as e:
        error_msg = f"Error fetching from NocoDB: {str(e)}"
        if e.response:
            error_msg += f" - Response: {e.response.text}"
        logger.error(error_msg)
        return None


def save_order_to_nocodb(order_data, conid, order_type, price, quantity, side, tif):
    """Save IBKR order details to the NocoDB orders table."""
    if not all([NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_ORDERS_TABLE_ID]):
        logger.error("NocoDB orders table configuration is incomplete.")
        raise ValueError("NocoDB orders table configuration is incomplete")

    url = f"{NOCODB_BASE_URL}/api/v2/tables/{NOCODB_ORDERS_TABLE_ID}/records"
    headers = {
        "xc-token": NOCODB_API_TOKEN,
        "Content-Type": "application/json"
    }

    logger.debug(f'Order URL: {url}')
    logger.debug(f'Order conid: {conid}')
    logger.debug(f'NOCODB_ORDERS_TABLE_ID: {NOCODB_ORDERS_TABLE_ID}')

    payload = {
        "order_id": str(order_data["order_id"]),
        "conid": str(conid),
        "status": order_data["order_status"],
        "order_type": order_type,
        "price": float(price),
        "quantity": int(quantity),
        "side": side,
        "tif": tif,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "order_details": json.dumps(order_data)
    }
    logger.debug(f'Order payload: {payload}')

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Saved order {order_data.get('order_id')} to NocoDB")
    except requests.RequestException as e:
        error_msg = f"Error saving order to NocoDB: {str(e)}"
        if e.response:
            error_msg += f" - Response: {e.response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)


def save_trade_to_nocodb(order_id, conid, order_data):
    """Save trade details to the NocoDB trades table."""
    if not all([NOCODB_BASE_URL, NOCODB_API_TOKEN, NOCODB_TRADES_TABLE_ID]):
        logger.error("NocoDB trades table configuration is incomplete.")
        raise ValueError("NocoDB trades table configuration is incomplete")

    url = f"{NOCODB_BASE_URL}/api/v2/tables/{NOCODB_TRADES_TABLE_ID}/records"
    headers = {
        "xc-token": NOCODB_API_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "order_id": str(order_id),
        "conid": str(conid),
        "status": order_data.get("status", "Filled"),
        "price": float(order_data.get("price", 0)),
        "quantity": int(order_data.get("quantity", 0)),
        "side": order_data.get("side", ""),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "trade_details": json.dumps(order_data)  # Store full order data as JSON
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Saved trade for order {order_id} to NocoDB")
    except requests.RequestException as e:
        error_msg = f"Error saving trade to NocoDB: {str(e)}"
        if e.response:
            error_msg += f" - Response: {e.response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)


def save_notification_state(order_id, conid, notified):
    """Save notification state to NocoDB."""
    url = f"{NOCODB_BASE_URL}/api/v2/tables/{NOCODB_TRADES_TABLE_ID}/records"
    headers = {"xc-token": NOCODB_API_TOKEN, "Content-Type": "application/json"}
    payload = {
        "order_id": str(order_id),
        "conid": str(conid),
        "notified": notified,
        "updated_at": datetime.utcnow().isoformat() + "Z"
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.debug(f"Saved notification state for order {order_id}")
    except requests.RequestException as e:
        logger.error(f"Error saving notification state: {str(e)}")


def get_notification_state(order_id):
    """Retrieve notification state from NocoDB."""
    url = f"{NOCODB_BASE_URL}/api/v2/tables/{NOCODB_TRADES_TABLE_ID}/records"
    headers = {"xc-token": NOCODB_API_TOKEN}
    params = {"where": f"(order_id,eq,{str(order_id)})"}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("list") and len(data["list"]) > 0:
            return data["list"][0].get("notified", False)
        return False
    except requests.RequestException as e:
        logger.error(f"Error fetching notification state: {str(e)}")
        return False 