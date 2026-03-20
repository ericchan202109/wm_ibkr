import requests
from datetime import datetime
from webapp.config.settings import (
    logger, BASE_API_URL, ACCOUNT_ID, PENDING_ORDERS
)
from webapp.services.db_service import (
    save_order_to_nocodb, save_trade_to_nocodb, 
    save_notification_state, get_notification_state
)
from webapp.services.api_service import send_telegram_notification

def check_order_status():
    """Periodically check the status of all pending orders."""
    from webapp.services.api_service import is_authenticated
    
    if not is_authenticated():
        logger.warning("Not authenticated, skipping order status check.")
        return

    try:
        response = requests.get(f"{BASE_API_URL}/iserver/account/orders", verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to fetch orders: {response.text}")
            return

        orders = response.json().get("orders", [])
        for order in orders:
            order_id = order.get("orderId")
            status = order.get("status")
            conid = order.get("conid")

            if order_id in PENDING_ORDERS:
                if PENDING_ORDERS[order_id]["status"] != status:
                    logger.info(f"Order {order_id} status changed to {status}")
                    PENDING_ORDERS[order_id]["status"] = status

                    if status.lower() in ["filled", "api_filled"]:
                        # Check NocoDB for notification state
                        if not get_notification_state(order_id):
                            send_telegram_notification(order_id, conid, status)
                            save_trade_to_nocodb(order_id, conid, order)
                            save_notification_state(order_id, conid, True)
            else:
                if status.lower() in ["pending", "presubmitted", "submitted"]:
                    PENDING_ORDERS[order_id] = {
                        "conid": conid,
                        "status": status,
                        "notified": get_notification_state(order_id)
                    }
                    logger.debug(f"Tracking new order {order_id} with status {status}")

        # Clean up
        for order_id in list(PENDING_ORDERS.keys()):
            if PENDING_ORDERS[order_id]["status"].lower() in ["filled", "api_filled", "cancelled", "inactive"]:
                del PENDING_ORDERS[order_id]
                logger.debug(f"Removed order {order_id} from tracking")

    except requests.RequestException as e:
        logger.error(f"Error checking order status: {str(e)}")


def get_orders():
    """Get all orders from IBKR API."""
    try:
        response = requests.get(f"{BASE_API_URL}/iserver/account/orders", verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to fetch orders: {response.text}")
            return []
        return response.json().get("orders", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching orders: {str(e)}")
        return []


def cancel_order(order_id):
    """Cancel a specific order."""
    try:
        response = requests.delete(f"{BASE_API_URL}/iserver/account/{ACCOUNT_ID}/order/{order_id}", verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to cancel order {order_id}: {response.text}")
            return {"success": False, "message": response.text}
        return {"success": True, "data": response.json()}
    except requests.RequestException as e:
        logger.error(f"Error canceling order {order_id}: {str(e)}")
        return {"success": False, "message": str(e)} 