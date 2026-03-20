import io
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from decimal import Decimal
from stock_indicators import indicators, Quote
from webapp.config.settings import logger

def calculate_obv(df):
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


def normalize_value(value):
    """Normalize large numbers for display (B, M, etc.)."""
    if abs(value) >= 1_000_000_000:
        return value / 1_000_000_000, "B"
    elif abs(value) >= 1_000_000:
        return value / 1_000_000, "M"
    return value, ""


def process_indicators(indicators_dict, df):
    """Process indicators for chart display."""
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
        'obv_normalized': [x / (1_000_000_000 if obv_unit == "B" else 1_000_000 if obv_unit == "M" else 1) for x in indicators_dict['obv']],
        'obv_unit': obv_unit,
        'volume': df['Volume'] / 1_000_000
    }


def create_plotly_figure(df, processed_indicators, symbol):
    """Create a Plotly figure with multiple technical indicators."""
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
    """Generate a technical chart with multiple indicators.
    
    Fixed implementation to ensure proper handling of normalized values and to
    prevent issues with indicator calculations.
    """
    try:
        # Convert to Quote objects for stock_indicators library
        quotes = []
        
        for _, row in df.iterrows():
            # Handle potential nan values
            volume = max(0, row['Volume']) if not pd.isna(row['Volume']) else 0
            
            quote = Quote(
                date=row['Date'].to_pydatetime(),
                open=Decimal(str(row['Open'])),
                high=Decimal(str(row['High'])),
                low=Decimal(str(row['Low'])),
                close=Decimal(str(row['Close'])),
                volume=Decimal(str(volume))
            )
            quotes.append(quote)
        
        # Skip indicators calculation if not enough data
        if len(quotes) < 200:
            logger.warning(f"Not enough data points ({len(quotes)}) to calculate all indicators")
            
        # Calculate indicators with appropriate error handling
        indicators_dict = {
            'sma_20': indicators.get_sma(quotes, min(20, len(quotes))),
            'sma_50': indicators.get_sma(quotes, min(50, len(quotes))),
            'sma_200': indicators.get_sma(quotes, min(200, len(quotes))),
            'bb': indicators.get_bollinger_bands(quotes, min(20, len(quotes)), 2),
            'supertrend': indicators.get_super_trend(quotes, min(10, len(quotes)), 3),
            'macd': indicators.get_macd(quotes, min(12, len(quotes)), min(26, len(quotes)), min(9, len(quotes))),
            'rsi': indicators.get_rsi(quotes, min(14, len(quotes))),
            'atr': indicators.get_atr(quotes, min(14, len(quotes))),
            'obv': calculate_obv(df)
        }

        processed_indicators = process_indicators(indicators_dict, df)
        fig = create_plotly_figure(df, processed_indicators, symbol)
        
        # Write image to buffer
        buf = io.BytesIO()
        pio.write_image(fig, buf, format='jpeg', width=width, height=height)
        buf.seek(0)
        return buf
        
    except Exception as e:
        logger.error(f"Error generating technical chart: {str(e)}")
        raise


def get_stats(data, unit=""):
    """Calculate statistics for indicator values."""
    valid_data = [x for x in data if x is not None and not pd.isna(x)]
    
    if not valid_data:
        return {
            "latest": "N/A",
            "max": "N/A",
            "min": "N/A",
            "average": "N/A",
            "median": "N/A"
        }
        
    return {
        "latest": f"{valid_data[-1]:.2f}{unit}",
        "max": f"{max(valid_data):.2f}{unit}",
        "min": f"{min(valid_data):.2f}{unit}",
        "average": f"{sum(valid_data) / len(valid_data):.2f}{unit}",
        "median": f"{sorted(valid_data)[len(valid_data) // 2]:.2f}{unit}"
    } 