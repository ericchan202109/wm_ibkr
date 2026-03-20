import requests
import os
import urllib3
from flask import Flask, render_template, request, redirect, jsonify, session, send_file
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import io
import logging
from stock_indicators import indicators, Quote
from decimal import Decimal
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from minio import Minio
from minio.error import S3Error
import json
import uuid
import psycopg2
from functools import lru_cache
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import threading
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress SSL warnings (for development only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configuration
BASE_API_URL = "https://127.0.0.1:5055/v1/api"
ACCOUNT_ID = os.environ.get('IBKR_ACCOUNT_ID')
os.environ['PYTHONHTTPSVERIFY'] = '0'
FLASK_APP_URL = "https://localhost:5056"  # Base URL for this Flask app

# In-memory store for tracking orders (replace with NocoDB for persistence if needed)
PENDING_ORDERS = {}  # Format: {order_id: {"conid": int, "status": str, "notified": bool}}
# MinIO Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
# Add to Configuration section
NOCODB_ORDERS_TABLE_ID = os.environ.get('NOCODB_ORDERS_TABLE_ID')  # Add to .env file
MINIO_BUCKET = "n8n"
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# NocoDB Configuration
NOCODB_BASE_URL = "http://nocodb:8080"  # Adjust to NocoDB's internal port
NOCODB_API_TOKEN = os.environ.get('NOCODB_API_TOKEN')  # Add this to your .env file
NOCODB_BASE_ID =  os.environ.get('NOCODB_PROJECT_ID') # Replace with your NocoDB base ID
NOCODB_TABLE_ID =  os.environ.get('NOCODB_TABLE_ID') # Replace with your market_data table ID

logger.debug(NOCODB_ORDERS_TABLE_ID)
# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.urandom(24)


# Ensure MinIO bucket exists
def ensure_bucket():
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)


# Database helper functions
# Updated save_to_nocodb function for hosted NocoDB v2 API
def save_to_nocodb(conid, period, bar, data):
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


# Helper function to check authentication
def is_authenticated():
    try:
        r = requests.get(f"{BASE_API_URL}/tickle", verify=False, timeout=15)
        return r.status_code == 200
    except requests.RequestException:
        return False


# Refactored data fetching function
def fetch_market_data(conid, period, bar, outside_rth, mode='realtime'):
    logger.debug(f"Fetching data from NocoDB for {conid}, {period}, {bar}, {outside_rth}, {mode}")
    if mode == 'offline':
        cached_data = get_from_nocodb(conid, period, bar)
        logger.debug('inside')
        logger.debug(cached_data)
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


# Helper functions
def calculate_obv(df):
    obv = [0]
    for i in range(1, len(df)):
        if df['Close'].iloc[i] > df['Close'].iloc[i - 1]:
            obv.append(obv[-1] + df['Volume'].iloc[i])
        elif df['Close'].iloc[i] < df['Close'].iloc[i - 1]:
            obv.append(obv[-1] - df['Volume'].iloc[i])
        else:
            obv.append(obv[-1])
    return obv


def normalize_value(value):
    if abs(value) >= 1_000_000_000:
        return value / 1_000_000_000, "B"
    elif abs(value) >= 1_000_000:
        return value / 1_000_000, "M"
    return value, ""


def process_indicators(indicators_dict, df):
    obv_normalized, obv_unit = normalize_value(indicators_dict['obv'][-1])
    return {
        'sma_20': [float(s.sma) if s.sma else None for s in indicators_dict['sma_20']],
        'sma_50': [float(s.sma) if s.sma else None for s in indicators_dict['sma_50']],
        'sma_200': [float(s.sma) if s.sma else None for s in indicators_dict['sma_200']],
        'bb_upper': [float(b.upper_band) if b.upper_band else None for b in indicators_dict['bb']],
        'bb_lower': [float(b.lower_band) if b.lower_band else None for b in indicators_dict['bb']],
        'bb_median': [float(b.sma) if b.sma else None for b in indicators_dict['bb']],
        'st_values': [float(s.super_trend) if s.super_trend else None for s in indicators_dict['supertrend']],
        'st_direction': [1 if s.upper_band else -1 if s.lower_band else 0 for s in indicators_dict['supertrend']],
        'macd_line': [float(m.macd) if m.macd else None for m in indicators_dict['macd']],
        'signal_line': [float(m.signal) if m.signal else None for m in indicators_dict['macd']],
        'histogram': [float(m.histogram) if m.histogram else None for m in indicators_dict['macd']],
        'rsi': [float(r.rsi) if r.rsi else None for r in indicators_dict['rsi']],
        'atr': [float(a.atr) if a.atr else None for a in indicators_dict['atr']],
        'obv_normalized': [x / (1_000_000_000 if obv_unit == "B" else 1_000_000) for x in indicators_dict['obv']],
        'obv_unit': obv_unit,
        'volume': df['Volume'] / 1_000_000
    }


def create_plotly_figure(df, processed_indicators, symbol):
    dates = df['Date']
    latest = df.iloc[-1]
    latest_ohlc = f"Open: {latest['Open']:.2f} | High: {latest['High']:.2f} | Low: {latest['Low']:.2f} | Close: {latest['Close']:.2f}"

    fig = make_subplots(rows=7, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                        subplot_titles=(f"{symbol} Price", "SuperTrend", "Volume (M)", "MACD", "RSI",
                                        f"OBV ({processed_indicators['obv_unit']})", "ATR"),
                        row_heights=[0.35, 0.15, 0.1, 0.15, 0.15, 0.1, 0.1],
                        specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}],
                               [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}],
                               [{"secondary_y": True}]])

    def add_max_min_avg(data, row, name, yaxis="y", unit=""):
        if not data or all(x is None for x in data):
            return
        valid_data = [x for x in data if x is not None]
        if not valid_data:
            return
        max_val, min_val = max(valid_data), min(valid_data)
        avg_val = sum(valid_data) / len(valid_data)
        median_val = sorted(valid_data)[len(valid_data) // 2]
        max_idx = data.index(max_val) if max_val in data else None
        min_idx = data.index(min_val) if min_val in data else None
        max_text = f"Max: {max_val:.2f}{unit}"
        min_text = f"Min: {min_val:.2f}{unit}"
        avg_text = f"Avg: {avg_val:.2f}{unit}"
        median_text = f"Med: {median_val:.2f}{unit}"
        if max_idx is not None:
            fig.add_annotation(x=dates.iloc[max_idx], y=max_val, text=max_text, showarrow=True, arrowhead=1, row=row,
                               col=1, yref=yaxis)
        if min_idx is not None:
            fig.add_annotation(x=dates.iloc[min_idx], y=min_val, text=min_text, showarrow=True, arrowhead=1, row=row,
                               col=1, yref=yaxis)
        fig.add_hline(y=avg_val, line_dash="dash", line_color="gray", line_width=1, row=row, col=1,
                      annotation_text=avg_text, annotation_position="right")
        fig.add_hline(y=median_val, line_dash="dot", line_color="gray", line_width=1, row=row, col=1,
                      annotation_text=median_text, annotation_position="right")

    # Price Chart
    fig.add_trace(go.Candlestick(x=dates, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
                                 name=f'{symbol} Price', increasing_line_color='green', decreasing_line_color='red'),
                  row=1, col=1)
    fig.add_trace(
        go.Scatter(x=[dates.iloc[-1]], y=[latest['Close']], mode='markers+text', text=[f"{latest['Close']:.2f}"],
                   textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10), yaxis='y2'),
        row=1, col=1, secondary_y=True)
    add_max_min_avg(df['Close'].tolist(), 1, "Price")

    fig.add_trace(
        go.Scatter(x=dates, y=processed_indicators['sma_20'], mode='lines', name='SMA 20', line=dict(color='blue')),
        row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['sma_50'], mode='lines', name='SMA 50',
                             line=dict(color='rgb(47,203,13)', width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['sma_200'], mode='lines', name='SMA 200',
                             line=dict(color='rgb(240,130,21)', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['bb_upper'], mode='lines', name='BB Upper',
                             line=dict(color='rgb(33,150,243)', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['bb_lower'], mode='lines', name='BB Lower',
                             line=dict(color='rgb(33,150,243)', width=1), fill='tonexty',
                             fillcolor='rgba(33,150,243,0.1)'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['bb_median'], mode='lines', name='BB Median',
                             line=dict(color='rgb(255,109,0)', width=1)), row=1, col=1)

    # SuperTrend
    st_up = [v if d == 1 else None for v, d in
             zip(processed_indicators['st_values'], processed_indicators['st_direction'])]
    st_down = [v if d == -1 else None for v, d in
               zip(processed_indicators['st_values'], processed_indicators['st_direction'])]
    fig.add_trace(go.Scatter(x=dates, y=st_up, mode='lines', name='SuperTrend Up', line=dict(color='rgb(0,128,0)')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=dates, y=st_down, mode='lines', name='SuperTrend Down', line=dict(color='rgb(128,0,0)')),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=[dates.iloc[-1]], y=[processed_indicators['st_values'][-1]], mode='markers+text',
                             text=[f"{processed_indicators['st_values'][-1]:.2f}"],
                             textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10),
                             yaxis='y2'), row=2, col=1, secondary_y=True)
    add_max_min_avg(processed_indicators['st_values'], 2, "SuperTrend")

    # Volume
    colors = ['green' if df['Close'][i] >= df['Open'][i] else 'red' for i in range(len(df))]
    fig.add_trace(go.Bar(x=dates, y=processed_indicators['volume'], name='Volume', marker_color=colors), row=3, col=1)
    fig.add_trace(go.Scatter(x=[dates.iloc[-1]], y=[processed_indicators['volume'].iloc[-1]], mode='markers+text',
                             text=[f"{processed_indicators['volume'].iloc[-1]:.2f}M"],
                             textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10),
                             yaxis='y2'), row=3, col=1, secondary_y=True)
    add_max_min_avg(processed_indicators['volume'].tolist(), 3, "Volume", unit="M")

    # MACD
    fig.add_trace(go.Bar(x=dates, y=processed_indicators['histogram'], name='MACD Histogram', marker_color=[
        'rgb(34,171,148)' if h is not None and h > 0 and prev is not None and h >= prev else
        'rgb(172,229,220)' if h is not None and h > 0 else
        'rgb(252,203,205)' if h is not None and h < 0 and prev is not None and h <= prev else
        'rgb(255,82,82)' if h is not None and h < 0 else 'gray' for h, prev in
        zip(processed_indicators['histogram'], [0] + processed_indicators['histogram'][:-1])]), row=4, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['macd_line'], mode='lines', name='MACD',
                             line=dict(color='rgb(33,150,243)', width=1)), row=4, col=1)
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['signal_line'], mode='lines', name='Signal',
                             line=dict(color='rgb(255,109,0)', width=1)), row=4, col=1)
    fig.add_trace(go.Scatter(x=[dates.iloc[-1]], y=[processed_indicators['macd_line'][-1]], mode='markers+text',
                             text=[f"{processed_indicators['macd_line'][-1]:.2f}"],
                             textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10),
                             yaxis='y2'), row=4, col=1, secondary_y=True)
    add_max_min_avg(processed_indicators['macd_line'], 4, "MACD")

    # RSI
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['rsi'], mode='lines', name='RSI',
                             line=dict(color='rgb(126,87,194)', width=1)), row=5, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="rgb(120,123,134)", line_width=1, row=5, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="rgb(120,123,134)", line_width=1, row=5, col=1)
    fig.add_hrect(y0=30, y1=70, fillcolor="rgba(126,87,194,0.1)", line_width=0, row=5, col=1)
    fig.add_trace(go.Scatter(x=[dates.iloc[-1]], y=[processed_indicators['rsi'][-1]], mode='markers+text',
                             text=[f"{processed_indicators['rsi'][-1]:.2f}"],
                             textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10),
                             yaxis='y2'), row=5, col=1, secondary_y=True)
    add_max_min_avg(processed_indicators['rsi'], 5, "RSI")

    # OBV
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['obv_normalized'], mode='lines', name='OBV',
                             line=dict(color='rgb(33,150,243)', width=1)), row=6, col=1)
    fig.add_trace(go.Scatter(x=[dates.iloc[-1]], y=[processed_indicators['obv_normalized'][-1]], mode='markers+text',
                             text=[
                                 f"{processed_indicators['obv_normalized'][-1]:.2f}{processed_indicators['obv_unit']}"],
                             textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10),
                             yaxis='y2'), row=6, col=1, secondary_y=True)
    add_max_min_avg(processed_indicators['obv_normalized'], 6, "OBV", unit=processed_indicators['obv_unit'])

    # ATR
    fig.add_trace(go.Scatter(x=dates, y=processed_indicators['atr'], mode='lines', name='ATR',
                             line=dict(color='rgb(128,25,34)', width=1)), row=7, col=1)
    fig.add_trace(go.Scatter(x=[dates.iloc[-1]], y=[processed_indicators['atr'][-1]], mode='markers+text',
                             text=[f"{processed_indicators['atr'][-1]:.2f}"],
                             textposition="middle right", showlegend=False, marker=dict(color='yellow', size=10),
                             yaxis='y2'), row=7, col=1, secondary_y=True)
    add_max_min_avg(processed_indicators['atr'], 7, "ATR")

    # Update layout
    fig.update_layout(
        title=dict(text=f'{symbol} <br><sup>{latest_ohlc}</sup>', x=0.5, xanchor='center', font=dict(size=20)),
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        template='plotly_white',
        height=1400,
        font=dict(size=16),
        legend=dict(x=0.01, y=-0.05, xanchor="left", yanchor="top", orientation="h"),
        xaxis7_title="Date"
    )
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1, ticklabelposition="outside")
    fig.update_yaxes(title_text="SuperTrend", row=2, col=1, ticklabelposition="outside")
    fig.update_yaxes(title_text="Volume (M)", row=3, col=1, ticklabelposition="outside")
    fig.update_yaxes(title_text="MACD", row=4, col=1, ticklabelposition="outside")
    fig.update_yaxes(title_text="RSI", row=5, col=1, ticklabelposition="outside")
    fig.update_yaxes(title_text=f"OBV ({processed_indicators['obv_unit']})", row=6, col=1, ticklabelposition="outside")
    fig.update_yaxes(title_text="ATR", row=7, col=1, ticklabelposition="outside")
    for i in range(1, 8):
        fig.update_yaxes(showgrid=False, title_text="", row=i, col=1, secondary_y=True, ticklabelposition="outside")

    return fig


def generate_technical_chart(df, symbol, width, height):
    quotes = [Quote(date=row['Date'].to_pydatetime(), open=Decimal(str(row['Open'])),
                    high=Decimal(str(row['High'])), low=Decimal(str(row['Low'])),
                    close=Decimal(str(row['Close'])), volume=Decimal(str(row['Volume'])))
              for _, row in df.iterrows()]

    indicators_dict = {
        'sma_20': indicators.get_sma(quotes, 20),
        'sma_50': indicators.get_sma(quotes, 50),
        'sma_200': indicators.get_sma(quotes, 200),
        'bb': indicators.get_bollinger_bands(quotes, 20, 2),
        'supertrend': indicators.get_super_trend(quotes, 10, 3),
        'macd': indicators.get_macd(quotes, 12, 26, 9),
        'rsi': indicators.get_rsi(quotes, 14),
        'atr': indicators.get_atr(quotes, 14),
        'obv': calculate_obv(df)
    }

    processed_indicators = process_indicators(indicators_dict, df)
    fig = create_plotly_figure(df, processed_indicators, symbol)
    buf = io.BytesIO()
    pio.write_image(fig, buf, format='jpeg', width=width, height=height)
    buf.seek(0)
    return buf


def get_stats(data, unit=""):
    valid_data = [x for x in data if x is not None]
    return {
        "latest": f"{valid_data[-1]:.2f}{unit}" if valid_data else "N/A",
        "max": f"{max(valid_data):.2f}{unit}" if valid_data else "N/A",
        "min": f"{min(valid_data):.2f}{unit}" if valid_data else "N/A",
        "average": f"{sum(valid_data) / len(valid_data):.2f}{unit}" if valid_data else "N/A",
        "median": f"{sorted(valid_data)[len(valid_data) // 2]:.2f}{unit}" if valid_data else "N/A"
    }


# Routes
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if is_authenticated():
            session['authenticated'] = True
            return redirect("/")
        else:
            return render_template("login.html", error="Authentication failed. Ensure IBKR Gateway is running.")
    return render_template("login.html", error=None)


@app.route("/logout")
def logout():
    session.pop('authenticated', None)
    return redirect("/login")


@app.route("/")
def dashboard():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.get(f"{BASE_API_URL}/portfolio/accounts", verify=False)
        if r.status_code != 200:
            return render_template("error.html", message=f"Failed to fetch accounts: {r.text}")
        accounts = r.json()
        account = accounts[0]
        account_id = account["id"]

        r = requests.get(f"{BASE_API_URL}/portfolio/{account_id}/summary", verify=False)
        summary = r.json() if r.status_code == 200 else {}

        r = requests.get(f"{BASE_API_URL}/portfolio/{account_id}/positions/0", verify=False)
        positions = r.json() if r.status_code == 200 and r.content else []

        for pos in positions:
            conid = pos["conid"]
            data = {"conids": [conid]}
            r = requests.post(f"{BASE_API_URL}/trsrv/secdef", json=data, verify=False)
            if r.status_code == 200:
                contract = r.json().get("secdef", [{}])[0]
                pos["contract_name"] = contract.get("desc", pos.get("symbol", "Unknown"))

        return render_template("dashboard.html", account=account, summary=summary, positions=positions)
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/search", methods=["GET"])
def search_symbol():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    symbol = request.args.get('symbol', '').strip()
    stocks = []
    if symbol:
        try:
            r = requests.get(f"{BASE_API_URL}/iserver/secdef/search?symbol={symbol}&name=true", verify=False)
            stocks = r.json() if r.status_code == 200 else []
        except requests.RequestException as e:
            return render_template("search.html", stocks=[], error=f"Error: {str(e)}")
    return render_template("search.html", stocks=stocks, error=None)


@app.route("/marketdata/<conid>", methods=["GET"])
def market_data(conid):
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    period = request.args.get('period', '1d')
    bar = request.args.get('bar', '1h')
    try:
        r = requests.get(f"{BASE_API_URL}/iserver/marketdata/history?conid={conid}&period={period}&bar={bar}",
                         verify=False)
        data = r.json() if r.status_code == 200 else {}
        return render_template("marketdata.html", data=data, conid=conid)
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/orders")
def view_orders():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.get(f"{BASE_API_URL}/iserver/account/orders", verify=False)
        orders = r.json().get("orders", []) if r.status_code == 200 else []
        return render_template("orders.html", orders=orders)
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/order/cancel/<order_id>")
def cancel_order(order_id):
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.delete(f"{BASE_API_URL}/iserver/account/{ACCOUNT_ID}/order/{order_id}", verify=False)
        return jsonify(r.json() if r.status_code == 200 else {"error": r.text})
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route("/account/summary")
def account_summary():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.get(f"{BASE_API_URL}/portfolio/accounts", verify=False)
        accounts = r.json() if r.status_code == 200 else []
        account_id = accounts[0]["id"] if accounts else ACCOUNT_ID
        r = requests.get(f"{BASE_API_URL}/portfolio/{account_id}/summary", verify=False)
        summary = r.json() if r.status_code == 200 else {}
        return render_template("account_summary.html", summary=summary)
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/positions")
def positions():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.get(f"{BASE_API_URL}/portfolio/{ACCOUNT_ID}/positions/0", verify=False)
        positions = r.json() if r.status_code == 200 and r.content else []
        return render_template("positions.html", positions=positions)
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/contract/<conid>")
def contract_details(conid):
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        data = {"conids": [conid]}
        r = requests.post(f"{BASE_API_URL}/trsrv/secdef", json=data, verify=False)
        contract = r.json().get("secdef", [{}])[0] if r.status_code == 200 else {}
        return render_template("contract.html", contract=contract)
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/scanner", methods=["GET", "POST"])
def scanner():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.get(f"{BASE_API_URL}/iserver/scanner/params", verify=False)
        params = r.json() if r.status_code == 200 else {}

        if request.method == "POST":
            data = {
                "instrument": request.form.get("instrument"),
                "location": request.form.get("location"),
                "type": request.form.get("type"),
                "filter": [{"code": request.form.get("filter_code"), "value": request.form.get("filter_value")}]
            }
            r = requests.post(f"{BASE_API_URL}/iserver/scanner/run", json=data, verify=False)
            results = r.json() if r.status_code == 200 else []
            return render_template("scanner.html", params=params, results=results)
        return render_template("scanner.html", params=params, results=[])
    except requests.RequestException as e:
        return render_template("error.html", message=f"Error: {str(e)}")


@app.route("/tickle")
def tickle():
    if not session.get('authenticated') or not is_authenticated():
        return redirect("/login")
    try:
        r = requests.get(f"{BASE_API_URL}/tickle", verify=False)
        return jsonify({"status": "alive" if r.status_code == 200 else "error", "response": r.text})
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reauthenticate")
def reauthenticate():
    if not session.get('authenticated'):
        return redirect("/login")
    try:
        r = requests.post(f"{BASE_API_URL}/iserver/reauthenticate", verify=False)
        return jsonify(r.json() if r.status_code == 200 else {"error": r.text})
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


@app.route('/market_history/<conid>')
def market_history_json(conid):
    if not is_authenticated():
        return jsonify({"error": "User not authenticated"}), 401

    period = request.args.get('period', '1min')
    bar = request.args.get('bar', '5min')
    outside_rth = request.args.get('outsideRth', 'true')
    mode = request.args.get('mode', 'realtime')

    try:
        data = fetch_market_data(conid, period, bar, outside_rth, mode)
        return jsonify(data)
    except requests.RequestException as e:
        logger.error(f"Error fetching market data: {e}")
        return jsonify({"error": "Unable to fetch market data", "message": str(e)}), 500


@app.route("/api/authenticate", methods=["GET"])
def api_authenticate():
    if is_authenticated():
        session['authenticated'] = True
        return jsonify({"status": "authenticated"})
    return jsonify({"error": "Authentication failed"}), 401


@app.route("/history_chart/<conid>", methods=["GET"])
def history_chart(conid):
    period = request.args.get('period', '1Y')
    bar = request.args.get('bar', '1d')
    outside_rth = request.args.get('outsideRth', 'false')
    mode = request.args.get('mode', 'realtime')
    width = int(request.args.get('width', 1920))
    height = int(request.args.get('height', 2048))
    if not is_authenticated() and mode == 'realtime':
        return jsonify({"error": "User not authenticated"}), 401

    try:
        ensure_bucket()
        data_json = fetch_market_data(conid, period, bar, outside_rth, mode)

        symbol = data_json.get('symbol', 'Instrument')
        df = pd.DataFrame(data_json['data'])
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df.rename(columns={'t': 'Date', 'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'},
                  inplace=True)
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

        chart_buf = generate_technical_chart(df, symbol, width, height)
        chart_url = save_buffer_to_minio(chart_buf, "market_charts")

        return jsonify({"chart_url": chart_url})

    except Exception as e:
        logger.error(f"Error in history_chart: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/history_chart_stats/<conid>", methods=["GET"])
def history_chart_stats(conid):
    if not is_authenticated():
        return jsonify({"error": "User not authenticated"}), 401

    period = request.args.get('period', '1Y')
    bar = request.args.get('bar', '1d')
    outside_rth = request.args.get('outsideRth', 'false')
    mode = request.args.get('mode', 'realtime')

    try:
        data_json = fetch_market_data(conid, period, bar, outside_rth, mode)
        df = pd.DataFrame(data_json['data'])
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df.rename(columns={'t': 'Date', 'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'},
                  inplace=True)
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

        quotes = [Quote(date=row['Date'].to_pydatetime(), open=Decimal(str(row['Open'])),
                        high=Decimal(str(row['High'])), low=Decimal(str(row['Low'])),
                        close=Decimal(str(row['Close'])), volume=Decimal(str(row['Volume'])))
                  for _, row in df.iterrows()]

        indicators_dict = {
            'sma_20': indicators.get_sma(quotes, 20),
            'sma_50': indicators.get_sma(quotes, 50),
            'sma_200': indicators.get_sma(quotes, 200),
            'bb': indicators.get_bollinger_bands(quotes, 20, 2),
            'supertrend': indicators.get_super_trend(quotes, 10, 3),
            'macd': indicators.get_macd(quotes, 12, 26, 9),
            'rsi': indicators.get_rsi(quotes, 14),
            'atr': indicators.get_atr(quotes, 14),
            'obv': calculate_obv(df)
        }

        processed = process_indicators(indicators_dict, df)
        latest = df.iloc[-1]

        stats = {
            "OHLC": {
                "Open": f"{latest['Open']:.2f}",
                "High": f"{latest['High']:.2f}",
                "Low": f"{latest['Low']:.2f}",
                "Close": get_stats(df['Close'].tolist())
            },
            "SMA_20": get_stats(processed['sma_20']),
            "SMA_50": get_stats(processed['sma_50']),
            "SMA_200": get_stats(processed['sma_200']),
            "BB_Upper": get_stats(processed['bb_upper']),
            "BB_Lower": get_stats(processed['bb_lower']),
            "BB_Median": get_stats(processed['bb_median']),
            "SuperTrend": get_stats(processed['st_values']),
            "Volume": get_stats(processed['volume'].tolist(), "M"),
            "MACD": get_stats(processed['macd_line']),
            "Signal": get_stats(processed['signal_line']),
            "Histogram": get_stats(processed['histogram']),
            "RSI": get_stats(processed['rsi']),
            "OBV": get_stats(processed['obv_normalized'], processed['obv_unit']),
            "ATR": get_stats(processed['atr'])
        }

        return jsonify({"stats": stats})

    except Exception as e:
        logger.error(f"Error in history_chart_stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Add to configuration section
NOCODB_TRADES_TABLE_ID = os.environ.get('NOCODB_TRADES_TABLE_ID')  # Add to .env file

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


def check_order_status():
    """Periodically check the status of all pending orders."""
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

# Add to configuration section
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL', 'http://n8n:5678/webhook/order-filled')

def send_telegram_notification(order_id, conid, status):
    """Send a Telegram notification via n8n webhook."""
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


# ... (other imports and config raemain unchanged)




# Add save function
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

    logger.info('order headers', headers)
    logger.info('order data', order_data)

    logger.info(f'order url: {url}')
    logger.info(f'order conid: {conid}')
    logger.info(f'NOCODB_ORDERS_TABLE_ID: {NOCODB_ORDERS_TABLE_ID}')

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
    logger.info('payload', payload)

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Saved order {order_data.get('orderId')} to NocoDB")
    except requests.RequestException as e:
        error_msg = f"Error saving order to NocoDB: {str(e)}"
        if e.response:
            error_msg += f" - Response: {e.response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)





@app.route("/order", methods=["POST","GET"])
def place_order():
    logger.debug("place_order")
    # Uncomment for production to enforce authentication
    # if not session.get('authenticated') or not is_authenticated():
    #     return redirect("/login")

    if request.method == "POST":
        try:
            # Extract form data
            conid = int(request.form.get('conid'))
            order_type = request.form.get('orderType', 'LMT')
            price = float(request.form.get('price'))
            quantity = int(request.form.get('quantity'))
            side = request.form.get('side')
            tif = request.form.get('tif', 'GTC')

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
            r = requests.post(f"{BASE_API_URL}/iserver/account/{ACCOUNT_ID}/orders", json=data, verify=False)

            if r.status_code != 200:
                logger.error(f"Failed to place order: {r.text}")
                return jsonify({"error": r.text}), r.status_code

            result = r.json()
            logger.debug(f"IBKR order response: {result}")

            # Save order to NocoDB if response contains order details
            if  len(result) > 0:
                order_response = result[0]
                logger.debug(f"Order response: {order_response}")
                save_order_to_nocodb(
                    order_data=order_response,
                    conid=conid,
                    order_type=order_type,
                    price=price,
                    quantity=quantity,
                    side=side,
                    tif=tif
                )

            return jsonify(result)
        except requests.RequestException as e:
            logger.error(f"Error placing order: {str(e)}")
            return jsonify({"error": str(e)}), 500
        except ValueError as e:
            logger.error(f"Invalid input: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return jsonify({"error": str(e)}), 500

    return render_template("order.html")


# Initialize scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(check_order_status, 'interval', seconds=20)
scheduler.start()

# Ensure scheduler shuts down gracefully
def shutdown_scheduler():
    scheduler.shutdown()

# Modify app initialization to handle scheduler
if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=5056, ssl_context=('/app/webapp/cert.pem', '/app/webapp/key.pem'), debug=True)
    finally:
        shutdown_scheduler()