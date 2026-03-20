import os
import logging
import urllib3

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress SSL warnings (for development only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# IBKR API Configuration
BASE_API_URL = "https://127.0.0.1:5000/v1/api"
ACCOUNT_ID = os.environ.get('IBKR_ACCOUNT_ID')
os.environ['PYTHONHTTPSVERIFY'] = '0'
FLASK_APP_URL = "https://localhost:5056"

# MinIO Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET = "n8n"

# NocoDB Configuration
NOCODB_BASE_URL = "http://nocodb:8080"  # Adjust to NocoDB's internal port
NOCODB_API_TOKEN = os.environ.get('NOCODB_API_TOKEN')
NOCODB_BASE_ID = os.environ.get('NOCODB_PROJECT_ID')
NOCODB_TABLE_ID = os.environ.get('NOCODB_TABLE_ID')
NOCODB_ORDERS_TABLE_ID = os.environ.get('NOCODB_ORDERS_TABLE_ID')
NOCODB_TRADES_TABLE_ID = os.environ.get('NOCODB_TRADES_TABLE_ID')

# N8N Configuration
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL', 'http://n8n:5678/webhook/order-filled')

# In-memory store for tracking orders
PENDING_ORDERS = {}  # Format: {order_id: {"conid": int, "status": str, "notified": bool}}

# Order defaults
DEFAULT_TIF = "DAY"  # Changed from GTC to DAY as per requirement

# Price adjustment settings for the price management algorithm
PRICE_ADJUST_BUY_PERCENT = float(os.environ.get('PRICE_ADJUST_BUY_PERCENT', '-0.5'))  # Default 0.5% lower for buy
PRICE_ADJUST_SELL_PERCENT = float(os.environ.get('PRICE_ADJUST_SELL_PERCENT', '0.5'))  # Default 0.5% higher for sell 