"""
Chart Generator Service for Multi-Timeframe Technical Analysis.

Generates Plotly charts with 7 panels based on reference/webapp/services/chart_service.py.
Exports charts as images for LLM vision analysis.
"""
import io
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
import logging

from backend.config.settings import settings
from backend.services.ibkr_service import IBKRService
from backend.services.minio_service import MinIOService

logger = logging.getLogger(__name__)


def normalize_value(value: float) -> Tuple[float, str]:
    """Normalize large numbers for display (B, M, K)."""
    if abs(value) >= 1_000_000_000:
        return value / 1_000_000_000, "B"
    elif abs(value) >= 1_000_000:
        return value / 1_000_000, "M"
    elif abs(value) >= 1_000:
        return value / 1_000, "K"
    return value, ""


def calculate_obv(df: pd.DataFrame) -> list:
    """Calculate On-Balance Volume (OBV)."""
    obv = [0]
    for i in range(1, len(df)):
        if df['Close'].iloc[i] > df['Close'].iloc[i - 1]:
            obv.append(obv[-1] + df['Volume'].iloc[i])
        elif df['Close'].iloc[i] < df['Close'].iloc[i - 1]:
            obv.append(obv[-1] - df['Volume'].iloc[i])
        else:
            obv.append(obv[-1])
    return obv


def calculate_sma(df: pd.DataFrame, period: int) -> pd.Series:
    """Calculate Simple Moving Average."""
    return df['Close'].rolling(window=period).mean()


def calculate_ema(df: pd.DataFrame, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return df['Close'].ewm(span=period, adjust=False).mean()


def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate Bollinger Bands."""
    sma = calculate_sma(df, period)
    std = df['Close'].rolling(window=period).std()
    upper = sma + (std * std_dev)
    lower = sma - (std * std_dev)
    return upper, sma, lower


def calculate_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0) -> Tuple[list, list]:
    """Calculate SuperTrend indicator."""
    # Calculate ATR first
    high_low = df['High'] - df['Low']
    high_close = abs(df['High'] - df['Close'].shift())
    low_close = abs(df['Low'] - df['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    atr = true_range.rolling(period).mean()
    
    # Basic SuperTrend calculation
    hl_avg = (df['High'] + df['Low']) / 2
    upper_band = hl_avg + (multiplier * atr)
    lower_band = hl_avg - (multiplier * atr)
    
    supertrend = [None] * len(df)
    direction = [0] * len(df)  # 1 = uptrend, -1 = downtrend
    
    for i in range(period, len(df)):
        if df['Close'].iloc[i] > upper_band.iloc[i - 1]:
            supertrend[i] = lower_band.iloc[i]
            direction[i] = 1
        elif df['Close'].iloc[i] < lower_band.iloc[i - 1]:
            supertrend[i] = upper_band.iloc[i]
            direction[i] = -1
        else:
            supertrend[i] = supertrend[i - 1] if i > 0 and supertrend[i - 1] else lower_band.iloc[i]
            direction[i] = direction[i - 1] if i > 0 else 0
    
    return supertrend, direction


def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate MACD."""
    ema_fast = calculate_ema(df, fast)
    ema_slow = calculate_ema(df, slow)
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Relative Strength Index."""
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    high_low = df['High'] - df['Low']
    high_close = abs(df['High'] - df['Close'].shift())
    low_close = abs(df['Low'] - df['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    atr = true_range.rolling(period).mean()
    return atr


def create_plotly_figure(df: pd.DataFrame, symbol: str, timeframe: str) -> go.Figure:
    """
    Create a 7-panel Plotly figure with technical indicators.
    Based on reference/webapp/services/chart_service.py.
    """
    dates = df['Date'] if 'Date' in df.columns else df.index
    latest = df.iloc[-1]
    latest_ohlc = f"Open: {latest['Open']:.2f} | High: {latest['High']:.2f} | Low: {latest['Low']:.2f} | Close: {latest['Close']:.2f}"
    
    # Calculate all indicators
    sma_20 = calculate_sma(df, 20)
    sma_50 = calculate_sma(df, 50)
    sma_200 = calculate_sma(df, 200)
    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(df)
    st_values, st_direction = calculate_supertrend(df)
    macd_line, signal_line, histogram = calculate_macd(df)
    rsi = calculate_rsi(df)
    atr = calculate_atr(df)
    obv = calculate_obv(df)
    
    # Normalize OBV
    obv_normalized, obv_unit = normalize_value(obv[-1] if obv else 0)
    obv_normalized_series = [x / (1_000_000_000 if obv_unit == "B" else 1_000_000 if obv_unit == "M" else 1_000 if obv_unit == "K" else 1) for x in obv]
    
    # Normalize volume
    volume_m = df['Volume'] / 1_000_000
    
    # Create subplots
    fig = make_subplots(
        rows=7, cols=1, shared_xaxes=True, vertical_spacing=0.02,
        subplot_titles=(
            f"{symbol} Price ({timeframe})", 
            "SuperTrend", 
            "Volume (M)", 
            "MACD", 
            "RSI",
            f"OBV ({obv_unit})", 
            "ATR"
        ),
        row_heights=[0.35, 0.15, 0.1, 0.15, 0.15, 0.1, 0.1],
        specs=[[{"secondary_y": True}]] * 7
    )
    
    # Panel 1: Price with MA and BB
    fig.add_trace(go.Candlestick(
        x=dates, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
        name=f'{symbol} Price', increasing_line_color='green', decreasing_line_color='red'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(x=dates, y=sma_20, mode='lines', name='SMA 20', line=dict(color='blue', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=sma_50, mode='lines', name='SMA 50', line=dict(color='green', width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=sma_200, mode='lines', name='SMA 200', line=dict(color='orange', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=bb_upper, mode='lines', name='BB Upper', line=dict(color='lightblue', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=bb_lower, mode='lines', name='BB Lower', line=dict(color='lightblue', width=1), fill='tonexty', fillcolor='rgba(33,150,243,0.1)'), row=1, col=1)
    
    # Panel 2: SuperTrend
    st_up = [v if d == 1 else None for v, d in zip(st_values, st_direction)]
    st_down = [v if d == -1 else None for v, d in zip(st_values, st_direction)]
    fig.add_trace(go.Scatter(x=dates, y=st_up, mode='lines', name='SuperTrend Up', line=dict(color='green')), row=2, col=1)
    fig.add_trace(go.Scatter(x=dates, y=st_down, mode='lines', name='SuperTrend Down', line=dict(color='red')), row=2, col=1)
    
    # Panel 3: Volume
    colors = ['green' if df['Close'].iloc[i] >= df['Open'].iloc[i] else 'red' for i in range(len(df))]
    fig.add_trace(go.Bar(x=dates, y=volume_m, name='Volume', marker_color=colors), row=3, col=1)
    
    # Panel 4: MACD
    macd_colors = ['green' if h > 0 else 'red' for h in histogram]
    fig.add_trace(go.Bar(x=dates, y=histogram, name='MACD Histogram', marker_color=macd_colors), row=4, col=1)
    fig.add_trace(go.Scatter(x=dates, y=macd_line, mode='lines', name='MACD', line=dict(color='blue', width=1)), row=4, col=1)
    fig.add_trace(go.Scatter(x=dates, y=signal_line, mode='lines', name='Signal', line=dict(color='orange', width=1)), row=4, col=1)
    
    # Panel 5: RSI
    fig.add_trace(go.Scatter(x=dates, y=rsi, mode='lines', name='RSI', line=dict(color='purple', width=1)), row=5, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="gray", line_width=1, row=5, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="gray", line_width=1, row=5, col=1)
    
    # Panel 6: OBV
    fig.add_trace(go.Scatter(x=dates, y=obv_normalized_series, mode='lines', name='OBV', line=dict(color='blue', width=1)), row=6, col=1)
    
    # Panel 7: ATR
    fig.add_trace(go.Scatter(x=dates, y=atr, mode='lines', name='ATR', line=dict(color='darkred', width=1)), row=7, col=1)
    
    # Update layout
    fig.update_layout(
        title=dict(text=f'{symbol} ({timeframe})<br><sup>{latest_ohlc}</sup>', x=0.5, xanchor='center', font=dict(size=20)),
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        template='plotly_white',
        height=1400,
        width=1920,
        font=dict(size=12),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    # Update axes
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
    
    return fig


class ChartGenerator:
    """Service for generating multi-timeframe technical analysis charts."""
    
    def __init__(self):
        self.ibkr_service = IBKRService()
        self.minio_service = MinIOService()
        
    async def generate_chart(
        self,
        symbol: str,
        timeframe: str = "1d",
        period: int = 200
    ) -> Dict[str, Any]:
        """
        Generate a technical analysis chart for the given symbol and timeframe.
        
        Args:
            symbol: Stock symbol (e.g., "TSLA")
            timeframe: Timeframe ("1d", "1w", "1mo")
            period: Number of bars to fetch
            
        Returns:
            Dict with chart URLs and metadata
        """
        try:
            logger.info(f"Generating {timeframe} chart for {symbol} ({period} bars)")
            
            # 1. Fetch market data
            df = await self._fetch_market_data(symbol, timeframe, period)
            if df is None or len(df) < 20:
                raise ValueError(f"Insufficient data for {symbol}: {len(df) if df is not None else 0} bars")
            
            # 2. Create Plotly figure
            fig = create_plotly_figure(df, symbol, timeframe)
            
            # 3. Export to JPEG
            image_data = self._export_to_jpeg(fig)
            
            # 4. Export to HTML
            html_data = pio.to_html(fig, include_plotlyjs='cdn')
            
            # 5. Upload to MinIO (use temporary chart_id = 0 for signals)
            chart_url_jpeg, chart_url_html = await self.minio_service.upload_chart(
                chart_jpeg=image_data,
                chart_html=html_data,
                symbol=symbol,
                chart_id=0,  # Signals don't have a chart_id, use 0
                timeframe=timeframe
            )
            
            logger.info(f"Chart generated successfully: {chart_url_jpeg}")
            
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "chart_url_jpeg": chart_url_jpeg,
                "chart_url_html": chart_url_html,
                "generated_at": datetime.now(),
                "data_points": len(df),
                "latest_price": float(df['Close'].iloc[-1]),
                "latest_date": df.index[-1] if isinstance(df.index, pd.DatetimeIndex) else df['Date'].iloc[-1]
            }
            
        except Exception as e:
            logger.error(f"Error generating chart for {symbol} ({timeframe}): {str(e)}")
            raise
    
    async def _fetch_market_data(self, symbol: str, timeframe: str, period: int) -> Optional[pd.DataFrame]:
        """Fetch market data from IBKR."""
        try:
            # 1. Search for contract to get conid
            contracts = await self.ibkr_service.search_contracts(symbol)
            if not contracts:
                logger.error(f"No contracts found for symbol {symbol}")
                return None
            
            # Use first contract (usually the stock)
            conid = contracts[0].get('conid')
            if not conid:
                logger.error(f"No conid found in contract for {symbol}")
                return None
            
            logger.info(f"Found conid {conid} for {symbol}")
            
            # 2. Convert timeframe to IBKR format
            # IBKR uses period like "1y", "6m", "1w"
            # and bar like "1d", "1w", "1mo"
            if timeframe == "1d":
                ibkr_period = "1y"  # Get 1 year of daily data
                ibkr_bar = "1d"
            elif timeframe == "1w":
                ibkr_period = "2y"  # Get 2 years of weekly data
                ibkr_bar = "1w"
            else:  # "1M" or "1mo"
                ibkr_period = "5y"  # Get 5 years of monthly data
                ibkr_bar = "1mo"
            
            # 3. Fetch historical data
            data = await self.ibkr_service.get_historical_data(
                conid=conid,
                period=ibkr_period,
                bar=ibkr_bar
            )
            
            if not data or 'data' not in data:
                logger.error(f"No data returned from IBKR for {symbol} (conid: {conid})")
                return None
            
            # 4. Convert to DataFrame
            bars = data.get('data', [])
            if not bars:
                logger.error(f"Empty data array for {symbol}")
                return None
            
            df = pd.DataFrame(bars)
            
            # 5. Process columns
            # IBKR returns: t (time), o (open), h (high), l (low), c (close), v (volume)
            if 't' in df.columns:
                df['Date'] = pd.to_datetime(df['t'], unit='ms')
                df.set_index('Date', inplace=True)
            
            # Rename columns to standard format
            col_mapping = {
                'o': 'Open',
                'h': 'High',
                'l': 'Low',
                'c': 'Close',
                'v': 'Volume'
            }
            df.rename(columns=col_mapping, inplace=True)
            
            # Ensure we have enough data
            if len(df) < 20:
                logger.warning(f"Insufficient data for {symbol}: only {len(df)} bars")
                return None
            
            # Limit to requested period
            df = df.tail(period)
            
            logger.info(f"Fetched {len(df)} bars for {symbol} ({timeframe})")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching market data for {symbol}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _export_to_jpeg(self, fig: go.Figure) -> bytes:
        """Export Plotly figure to JPEG image."""
        try:
            # Use kaleido for image export
            image_bytes = pio.to_image(
                fig,
                format='jpeg',
                width=settings.CHART_WIDTH,
                height=settings.CHART_HEIGHT,
                engine='kaleido'
            )
            return image_bytes
        except Exception as e:
            logger.error(f"Error exporting chart to JPEG: {str(e)}")
            logger.warning("Kaleido may not be installed. Install with: pip install kaleido")
            raise

