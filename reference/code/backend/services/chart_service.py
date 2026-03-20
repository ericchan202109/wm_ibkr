"""Chart generation service using Plotly for technical analysis visualization."""
import io
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import ta

from backend.models.indicator import Indicator
from backend.config.settings import settings

logger = logging.getLogger(__name__)


class ChartService:
    """Service for generating technical analysis charts with Plotly."""
    
    def __init__(self):
        """Initialize the chart service."""
        try:
            scope = getattr(pio.defaults, 'kaleido_scope', None)
            if scope is None:
                scope = getattr(pio, 'kaleido').scope
            scope.mathjax = None
            chromium_args = list(getattr(scope, 'chromium_args', ()))
            for arg in ("--single-process", "--disable-gpu"):
                if arg not in chromium_args:
                    chromium_args.append(arg)
            scope.chromium_args = tuple(chromium_args)
            logger.debug("Kaleido defaults initialized for backend chart service")
        except Exception as e:
            logger.warning(f"Kaleido initialization warning: {e}")
        
    async def generate_chart(
        self,
        symbol: str,
        market_data: pd.DataFrame,
        indicators_list: List[Indicator],
        period: int,
        frequency: str
    ) -> Tuple[bytes, str]:
        """
        Generate a comprehensive technical analysis chart.
        
        Args:
            symbol: Stock symbol
            market_data: DataFrame with OHLCV data
            indicators_list: List of Indicator models to render
            period: Number of data points
            frequency: Data frequency (1D, 1W, etc.)
            
        Returns:
            Tuple of (JPEG bytes, HTML string)
        """
        try:
            # Prepare data
            df = market_data.tail(period).copy()
            df['Date'] = pd.to_datetime(df['date'] if 'date' in df.columns else df.index)
            
            # Calculate all indicators
            price_arrays = self._prepare_price_arrays(df)
            calculated_indicators = self._calculate_indicators(price_arrays, df, indicators_list)
            
            # Create figure
            fig = self._create_plotly_figure(df, calculated_indicators, symbol, indicators_list)
            
            # Export to HTML first (always works)
            html_string = pio.to_html(fig, include_plotlyjs='cdn', full_html=True)
            
            # Try to export to JPEG (may fail if Chromium not available)
            jpeg_bytes = None
            try:
                jpeg_buffer = io.BytesIO()
                pio.write_image(fig, jpeg_buffer, format='jpeg', width=1920, height=1400, scale=2)
                jpeg_buffer.seek(0)
                jpeg_bytes = jpeg_buffer.read()
                logger.info(f"Successfully generated JPEG for {symbol}")
            except Exception as e:
                logger.warning(f"Failed to generate JPEG for {symbol}: {e}. HTML chart still available.")
                # Create a simple placeholder image message as bytes
                jpeg_bytes = b"JPEG generation requires Chromium installation"
            
            return jpeg_bytes, html_string
            
        except Exception as e:
            logger.error(f"Error generating chart for {symbol}: {str(e)}")
            raise
    
    def _prepare_price_arrays(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Convert DataFrame to numpy arrays for TA-Lib."""
        return {
            'open': df['open'].astype(float).values,
            'high': df['high'].astype(float).values,
            'low': df['low'].astype(float).values,
            'close': df['close'].astype(float).values,
            'volume': df['volume'].astype(float).values
        }
    
    def _calculate_indicators(
        self,
        price_arrays: Dict[str, np.ndarray],
        df: pd.DataFrame,
        indicators_list: List[Indicator]
    ) -> Dict[str, Any]:
        """Calculate all requested technical indicators using TA-Lib."""
        results = {}
        close = price_arrays['close']
        high = price_arrays['high']
        low = price_arrays['low']
        volume = price_arrays['volume']
        
        for indicator in indicators_list:
            try:
                params = indicator.parameters
                ind_type = indicator.type.upper()
                
                if ind_type in ['MA', 'SMA']:
                    # Simple Moving Average
                    period = params.get('period', 20)
                    calc = ta.trend.sma_indicator(close, window=period)
                    results[f'{indicator.name}'] = calc.tolist()

                elif ind_type == 'EMA':
                    # Exponential Moving Average
                    period = params.get('period', 20)
                    calc = ta.trend.ema_indicator(close, window=period)
                    results[f'{indicator.name}'] = calc.tolist()

                elif ind_type == 'WMA':
                    # Weighted Moving Average
                    period = params.get('period', 20)
                    calc = ta.trend.wma_indicator(close, window=period)
                    results[f'{indicator.name}'] = calc.tolist()
                
                elif ind_type == 'BB':
                    # Bollinger Bands
                    period = params.get('period', 20)
                    std_dev = params.get('std_dev', 2)
                    upper = ta.volatility.bollinger_hband(close, window=period, window_dev=std_dev)
                    lower = ta.volatility.bollinger_lband(close, window=period, window_dev=std_dev)
                    middle = ta.volatility.bollinger_mavg(close, window=period)
                    results[f'{indicator.name}_upper'] = upper.tolist()
                    results[f'{indicator.name}_lower'] = lower.tolist()
                    results[f'{indicator.name}_middle'] = middle.tolist()
                
                elif ind_type == 'SUPERTREND':
                    # SuperTrend (manual calculation as ta library doesn't have it)
                    period = params.get('period', 10)
                    multiplier = params.get('multiplier', 3)
                    atr = ta.volatility.average_true_range(high, low, close, window=period)
                    hl_avg = (high + low) / 2
                    upper_band = hl_avg + (multiplier * atr)
                    lower_band = hl_avg - (multiplier * atr)
                    
                    # Simplified SuperTrend calculation
                    supertrend = np.where(close > upper_band, lower_band, upper_band)
                    direction = np.where(close > supertrend, 1, -1)
                    
                    results[f'{indicator.name}_values'] = supertrend.tolist()
                    results[f'{indicator.name}_direction'] = direction.tolist()
                
                elif ind_type == 'MACD':
                    # MACD
                    fast = params.get('fast_period', 12)
                    slow = params.get('slow_period', 26)
                    signal = params.get('signal_period', 9)
                    macd = ta.trend.macd(close, window_fast=fast, window_slow=slow)
                    signal_line = ta.trend.macd_signal(close, window_fast=fast, window_slow=slow, window_sign=signal)
                    histogram = ta.trend.macd_diff(close, window_fast=fast, window_slow=slow, window_sign=signal)
                    results[f'{indicator.name}_line'] = macd.tolist()
                    results[f'{indicator.name}_signal'] = signal_line.tolist()
                    results[f'{indicator.name}_histogram'] = histogram.tolist()
                
                elif ind_type == 'RSI':
                    # RSI
                    period = params.get('period', 14)
                    calc = ta.momentum.rsi(close, window=period)
                    results[f'{indicator.name}'] = calc.tolist()

                elif ind_type == 'ATR':
                    # ATR
                    period = params.get('period', 14)
                    calc = ta.volatility.average_true_range(high, low, close, window=period)
                    results[f'{indicator.name}'] = calc.tolist()
                
                logger.info(f"Calculated indicator: {indicator.name} ({ind_type})")
                
            except Exception as e:
                logger.error(f"Error calculating indicator {indicator.name}: {e}")
                continue
        
        # Calculate OBV (On-Balance Volume)
        results['obv'] = self._calculate_obv(df)
        
        return results
    
    def _calculate_obv(self, df: pd.DataFrame) -> List[float]:
        """Calculate On-Balance Volume."""
        obv = [0.0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i - 1]:
                obv.append(obv[-1] + df['volume'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i - 1]:
                obv.append(obv[-1] - df['volume'].iloc[i])
            else:
                obv.append(obv[-1])
        return obv
    
    def _create_plotly_figure(
        self,
        df: pd.DataFrame,
        calculated_indicators: Dict[str, Any],
        symbol: str,
        indicators_list: List[Indicator]
    ) -> go.Figure:
        """Create the complete Plotly figure with all indicators and subplots."""
        dates = df['Date']
        latest = df.iloc[-1]
        latest_ohlc = f"Open: {latest['open']:.2f} | High: {latest['high']:.2f} | Low: {latest['low']:.2f} | Close: {latest['close']:.2f}"
        
        # Determine subplot layout based on indicators
        has_macd = any(ind.type.upper() == 'MACD' for ind in indicators_list)
        has_rsi = any(ind.type.upper() == 'RSI' for ind in indicators_list)
        has_atr = any(ind.type.upper() == 'ATR' for ind in indicators_list)
        has_supertrend = any(ind.type.upper() == 'SUPERTREND' for ind in indicators_list)
        
        # Build subplot structure
        num_rows = 1  # Price chart
        subplot_titles = [f"{symbol} Price"]
        row_heights = [0.5]
        
        if has_supertrend:
            num_rows += 1
            subplot_titles.append("SuperTrend")
            row_heights.append(0.15)
        
        num_rows += 1  # Always show volume
        subplot_titles.append("Volume (M)")
        row_heights.append(0.1)
        
        if has_macd:
            num_rows += 1
            subplot_titles.append("MACD")
            row_heights.append(0.15)
        
        if has_rsi:
            num_rows += 1
            subplot_titles.append("RSI")
            row_heights.append(0.15)
        
        if has_atr:
            num_rows += 1
            subplot_titles.append("ATR")
            row_heights.append(0.1)
        
        # Normalize row heights
        total_height = sum(row_heights)
        row_heights = [h / total_height for h in row_heights]
        
        # Create subplots
        fig = make_subplots(
            rows=num_rows,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            subplot_titles=subplot_titles,
            row_heights=row_heights,
            specs=[[{"secondary_y": True}] for _ in range(num_rows)]
        )
        
        current_row = 1
        
        # Row 1: Price Chart with candlesticks
        fig.add_trace(
            go.Candlestick(
                x=dates,
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name=f'{symbol} Price',
                increasing_line_color='green',
                decreasing_line_color='red'
            ),
            row=current_row,
            col=1
        )
        
        # Add latest price marker
        fig.add_trace(
            go.Scatter(
                x=[dates.iloc[-1]],
                y=[latest['close']],
                mode='markers+text',
                text=[f"{latest['close']:.2f}"],
                textposition="middle right",
                showlegend=False,
                marker=dict(color='yellow', size=10),
                yaxis='y2'
            ),
            row=current_row,
            col=1,
            secondary_y=True
        )
        
        # Add Moving Averages, Bollinger Bands to price chart
        for indicator in indicators_list:
            ind_type = indicator.type.upper()
            
            if ind_type in ['MA', 'SMA', 'EMA', 'WMA']:
                if f'{indicator.name}' in calculated_indicators:
                    # Assign colors based on period
                    period = indicator.parameters.get('period', 20)
                    color = 'blue' if period <= 20 else 'green' if period <= 50 else 'orange'
                    fig.add_trace(
                        go.Scatter(
                            x=dates,
                            y=calculated_indicators[f'{indicator.name}'],
                            mode='lines',
                            name=indicator.name,
                            line=dict(color=color, width=2)
                        ),
                        row=current_row,
                        col=1
                    )
            
            elif ind_type == 'BB':
        # Bollinger Bands
                if f'{indicator.name}_upper' in calculated_indicators:
                    fig.add_trace(
                        go.Scatter(
                            x=dates,
                            y=calculated_indicators[f'{indicator.name}_upper'],
                            mode='lines',
                            name=f'{indicator.name} Upper',
                            line=dict(color='rgba(33,150,243,0.8)', width=1)
                        ),
                        row=current_row,
                        col=1
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=dates,
                            y=calculated_indicators[f'{indicator.name}_lower'],
                            mode='lines',
                            name=f'{indicator.name} Lower',
                            line=dict(color='rgba(33,150,243,0.8)', width=1),
                            fill='tonexty',
                            fillcolor='rgba(33,150,243,0.1)'
                        ),
                        row=current_row,
                        col=1
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=dates,
                            y=calculated_indicators[f'{indicator.name}_middle'],
                            mode='lines',
                            name=f'{indicator.name} Middle',
                            line=dict(color='rgba(255,109,0,0.8)', width=1)
                        ),
                        row=current_row,
                        col=1
                    )
        
        # Add max/min/avg annotations for price
        self._add_max_min_avg_annotations(fig, df['close'].tolist(), dates, current_row, "Price")
        
        current_row += 1
        
        # SuperTrend subplot
        if has_supertrend:
            for indicator in indicators_list:
                if indicator.type.upper() == 'SUPERTREND':
                    if f'{indicator.name}_values' in calculated_indicators:
                        values = calculated_indicators[f'{indicator.name}_values']
                        directions = calculated_indicators[f'{indicator.name}_direction']
                        
                        st_up = [v if d == 1 else None for v, d in zip(values, directions)]
                        st_down = [v if d == -1 else None for v, d in zip(values, directions)]
                        
                        fig.add_trace(
                            go.Scatter(
                                x=dates,
                                y=st_up,
                                mode='lines',
                                name=f'{indicator.name} Up',
                                line=dict(color='rgb(0,128,0)', width=2)
                            ),
                            row=current_row,
                            col=1
                        )
                        fig.add_trace(
                            go.Scatter(
                                x=dates,
                                y=st_down,
                                mode='lines',
                                name=f'{indicator.name} Down',
                                line=dict(color='rgb(128,0,0)', width=2)
                            ),
                            row=current_row,
                            col=1
                        )
                        
                        # Latest value marker
                        fig.add_trace(
                            go.Scatter(
                                x=[dates.iloc[-1]],
                                y=[values[-1]],
                                mode='markers+text',
                                text=[f"{values[-1]:.2f}"],
                                textposition="middle right",
                                showlegend=False,
                                marker=dict(color='yellow', size=10),
                                yaxis='y2'
                            ),
                            row=current_row,
                            col=1,
                            secondary_y=True
                        )
                        
                        self._add_max_min_avg_annotations(fig, values, dates, current_row, indicator.name)
            
            current_row += 1
        
        # Volume subplot
        colors = ['green' if df['close'].iloc[i] >= df['open'].iloc[i] else 'red' for i in range(len(df))]
        volume_m = df['volume'] / 1_000_000
        fig.add_trace(
            go.Bar(
                x=dates,
                y=volume_m,
                name='Volume',
                marker_color=colors
            ),
            row=current_row,
            col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=[dates.iloc[-1]],
                y=[volume_m.iloc[-1]],
                mode='markers+text',
                text=[f"{volume_m.iloc[-1]:.2f}M"],
                textposition="middle right",
                showlegend=False,
                marker=dict(color='yellow', size=10),
                yaxis='y2'
            ),
            row=current_row,
            col=1,
            secondary_y=True
        )
        
        self._add_max_min_avg_annotations(fig, volume_m.tolist(), dates, current_row, "Volume", unit="M")
        current_row += 1
        
        # MACD subplot
        if has_macd:
            for indicator in indicators_list:
                if indicator.type.upper() == 'MACD':
                    if f'{indicator.name}_line' in calculated_indicators:
                        histogram = calculated_indicators[f'{indicator.name}_histogram']
                        macd_line = calculated_indicators[f'{indicator.name}_line']
                        signal_line = calculated_indicators[f'{indicator.name}_signal']
                        
                        # MACD histogram with color coding
                        fig.add_trace(
                            go.Bar(
                                x=dates,
                                y=histogram,
                                name=f'{indicator.name} Histogram',
                                marker_color=[
                                    'rgb(34,171,148)' if h is not None and h > 0 and i > 0 and h >= histogram[i-1] else
                                    'rgb(172,229,220)' if h is not None and h > 0 else
                                    'rgb(252,203,205)' if h is not None and h < 0 and i > 0 and h <= histogram[i-1] else
                                    'rgb(255,82,82)' if h is not None and h < 0 else 'gray'
                                    for i, h in enumerate(histogram)
                                ]
                            ),
                            row=current_row,
                            col=1
                        )
                        
                        fig.add_trace(
                            go.Scatter(
                                x=dates,
                                y=macd_line,
                                mode='lines',
                                name=f'{indicator.name} Line',
                                line=dict(color='rgb(33,150,243)', width=1)
                            ),
                            row=current_row,
                            col=1
                        )
                        
                        fig.add_trace(
                            go.Scatter(
                                x=dates,
                                y=signal_line,
                                mode='lines',
                                name='Signal',
                                line=dict(color='rgb(255,109,0)', width=1)
                            ),
                            row=current_row,
                            col=1
                        )
                        
                        fig.add_trace(
                            go.Scatter(
                                x=[dates.iloc[-1]],
                                y=[macd_line[-1]],
                                mode='markers+text',
                                text=[f"{macd_line[-1]:.2f}"],
                                textposition="middle right",
                                showlegend=False,
                                marker=dict(color='yellow', size=10),
                                yaxis='y2'
                            ),
                            row=current_row,
                            col=1,
                            secondary_y=True
                        )
                        
                        self._add_max_min_avg_annotations(fig, macd_line, dates, current_row, indicator.name)
            
            current_row += 1
        
        # RSI subplot
        if has_rsi:
            for indicator in indicators_list:
                if indicator.type.upper() == 'RSI':
                    if f'{indicator.name}' in calculated_indicators:
                        rsi_values = calculated_indicators[f'{indicator.name}']
                        overbought = indicator.parameters.get('overbought', 70)
                        oversold = indicator.parameters.get('oversold', 30)
                        
                        fig.add_trace(
                            go.Scatter(
                                x=dates,
                                y=rsi_values,
                                mode='lines',
                                name=indicator.name,
                                line=dict(color='rgb(126,87,194)', width=2)
                            ),
                            row=current_row,
                            col=1
                        )
                        
                        fig.add_hline(
                            y=overbought,
                            line_dash="dash",
                            line_color="rgb(120,123,134)",
                            line_width=1,
                            row=current_row,
                            col=1
                        )
                        fig.add_hline(
                            y=oversold,
                            line_dash="dash",
                            line_color="rgb(120,123,134)",
                            line_width=1,
                            row=current_row,
                            col=1
                        )
                        fig.add_hrect(
                            y0=oversold,
                            y1=overbought,
                            fillcolor="rgba(126,87,194,0.1)",
                            line_width=0,
                            row=current_row,
                            col=1
                        )
                        
                        fig.add_trace(
                            go.Scatter(
                                x=[dates.iloc[-1]],
                                y=[rsi_values[-1]],
                                mode='markers+text',
                                text=[f"{rsi_values[-1]:.2f}"],
                                textposition="middle right",
                                showlegend=False,
                                marker=dict(color='yellow', size=10),
                                yaxis='y2'
                            ),
                            row=current_row,
                            col=1,
                            secondary_y=True
                        )
                        
                        self._add_max_min_avg_annotations(fig, rsi_values, dates, current_row, indicator.name)
            
            current_row += 1
        
        # ATR subplot
        if has_atr:
            for indicator in indicators_list:
                if indicator.type.upper() == 'ATR':
                    if f'{indicator.name}' in calculated_indicators:
                        atr_values = calculated_indicators[f'{indicator.name}']
                        
                        fig.add_trace(
                            go.Scatter(
                                x=dates,
                                y=atr_values,
                                mode='lines',
                                name=indicator.name,
                                line=dict(color='rgb(128,25,34)', width=2)
                            ),
                            row=current_row,
                            col=1
                        )
                        
                        fig.add_trace(
                            go.Scatter(
                                x=[dates.iloc[-1]],
                                y=[atr_values[-1]],
                                mode='markers+text',
                                text=[f"{atr_values[-1]:.2f}"],
                                textposition="middle right",
                                showlegend=False,
                                marker=dict(color='yellow', size=10),
                                yaxis='y2'
                            ),
                            row=current_row,
                            col=1,
                            secondary_y=True
                        )
                        
                        self._add_max_min_avg_annotations(fig, atr_values, dates, current_row, indicator.name)
        
        # Update layout
        fig.update_layout(
            title=dict(
                text=f'{symbol} Technical Analysis<br><sup>{latest_ohlc}</sup>',
                x=0.5,
                xanchor='center',
                font=dict(size=20)
            ),
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
            template='plotly_white',
            height=1400,
            font=dict(size=14),
            legend=dict(
                x=0.01,
                y=-0.05,
                xanchor="left",
                yanchor="top",
                orientation="h"
            )
        )
        
        # Update y-axes
        fig.update_yaxes(title_text="Price (USD)", row=1, col=1, ticklabelposition="outside")
        for i in range(1, num_rows + 1):
            fig.update_yaxes(showgrid=False, title_text="", row=i, col=1, secondary_y=True, ticklabelposition="outside")
        
        fig.update_xaxes(title_text="Date", row=num_rows, col=1)
        
        return fig
    
    def _add_max_min_avg_annotations(
        self,
        fig: go.Figure,
        data: List[float],
        dates: pd.Series,
        row: int,
        name: str,
        unit: str = ""
    ):
        """Add max, min, and average annotations to a subplot."""
        if not data or all(x is None for x in data):
            return
        
        valid_data = [x for x in data if x is not None]
        if not valid_data:
            return
        
        max_val = max(valid_data)
        min_val = min(valid_data)
        avg_val = sum(valid_data) / len(valid_data)
        
        # Find indices
        max_idx = data.index(max_val) if max_val in data else None
        min_idx = data.index(min_val) if min_val in data else None
        
        # Add annotations
        if max_idx is not None and max_idx < len(dates):
            fig.add_annotation(
                x=dates.iloc[max_idx],
                y=max_val,
                text=f"Max: {max_val:.2f}{unit}",
                showarrow=True,
                arrowhead=1,
                row=row,
                col=1
            )
        
        if min_idx is not None and min_idx < len(dates):
            fig.add_annotation(
                x=dates.iloc[min_idx],
                y=min_val,
                text=f"Min: {min_val:.2f}{unit}",
                showarrow=True,
                arrowhead=1,
                row=row,
                col=1
            )
        
        # Add average line
        fig.add_hline(
            y=avg_val,
            line_dash="dash",
            line_color="gray",
            line_width=1,
            row=row,
            col=1,
            annotation_text=f"Avg: {avg_val:.2f}{unit}",
            annotation_position="right"
        )
