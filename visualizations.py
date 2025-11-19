"""
Stock Market Data Visualizations

Provides interactive charts for stock price and technical indicator analysis.
Uses Plotly for interactive visualizations.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from typing import Optional, List
import numpy as np


def create_candlestick_chart(
    df: pd.DataFrame,
    ticker: str,
    title: Optional[str] = None,
    show_volume: bool = True,
    show_sma: bool = True,
    height: int = 800
) -> go.Figure:
    """
    Create an interactive candlestick chart with volume and SMAs.
    
    Args:
        df: DataFrame with columns: date, open, high, low, close, volume, sma30, sma60
        ticker: Stock ticker symbol
        title: Chart title (optional)
        show_volume: Whether to show volume bars
        show_sma: Whether to show SMA lines
        height: Chart height in pixels
        
    Returns:
        Plotly Figure object
    """
    if title is None:
        title = f"{ticker} Price Chart with Technical Indicators"
    
    # Create subplots: 2 rows if showing volume, 1 otherwise
    rows = 2 if show_volume else 1
    row_heights = [0.7, 0.3] if show_volume else [1.0]
    
    fig = make_subplots(
        rows=rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=(f'{ticker} Price', 'Volume') if show_volume else (f'{ticker} Price',)
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ),
        row=1, col=1
    )
    
    # Add SMA lines if requested and available
    if show_sma:
        if 'sma30' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df['sma30'],
                    name='SMA 30',
                    line=dict(color='orange', width=2),
                    opacity=0.7
                ),
                row=1, col=1
            )
        
        if 'sma60' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df['sma60'],
                    name='SMA 60',
                    line=dict(color='blue', width=2),
                    opacity=0.7
                ),
                row=1, col=1
            )
    
    # Add volume bars if requested
    if show_volume and 'volume' in df.columns:
        colors = ['red' if close < open else 'green' 
                 for close, open in zip(df['close'], df['open'])]
        
        fig.add_trace(
            go.Bar(
                x=df['date'],
                y=df['volume'],
                name='Volume',
                marker_color=colors,
                opacity=0.5
            ),
            row=2, col=1
        )
    
    # Update layout
    fig.update_layout(
        title=title,
        yaxis_title='Price ($)',
        xaxis_rangeslider_visible=False,
        height=height,
        hovermode='x unified',
        template='plotly_dark'
    )
    
    if show_volume:
        fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    return fig


def create_technical_indicators_dashboard(
    df: pd.DataFrame,
    ticker: str,
    title: Optional[str] = None,
    height: int = 1200
) -> go.Figure:
    """
    Create a comprehensive technical indicators dashboard.
    
    Args:
        df: DataFrame with price and indicator columns
        ticker: Stock ticker symbol
        title: Dashboard title (optional)
        height: Chart height in pixels
        
    Returns:
        Plotly Figure with multiple subplots
    """
    if title is None:
        title = f"{ticker} Technical Analysis Dashboard"
    
    # Create 5 subplots: Price + Bollinger, MACD, RSI, CCI, DX
    fig = make_subplots(
        rows=5, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.35, 0.15, 0.15, 0.15, 0.15],
        subplot_titles=(
            f'{ticker} Price with Bollinger Bands',
            'MACD',
            'RSI (30)',
            'CCI (30)',
            'DX (30)'
        )
    )
    
    # Row 1: Price with Bollinger Bands
    fig.add_trace(
        go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ),
        row=1, col=1
    )
    
    if 'bollub' in df.columns and 'bolllb' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['bollub'],
                name='Upper Band',
                line=dict(color='rgba(250, 128, 114, 0.5)', width=1),
                fill=None
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['bolllb'],
                name='Lower Band',
                line=dict(color='rgba(250, 128, 114, 0.5)', width=1),
                fill='tonexty',
                fillcolor='rgba(250, 128, 114, 0.1)'
            ),
            row=1, col=1
        )
    
    # Row 2: MACD
    if 'macd' in df.columns:
        # Color bars based on positive/negative
        colors = ['green' if val >= 0 else 'red' for val in df['macd']]
        
        fig.add_trace(
            go.Bar(
                x=df['date'],
                y=df['macd'],
                name='MACD',
                marker_color=colors,
                opacity=0.7
            ),
            row=2, col=1
        )
        
        # Add zero line
        fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)
    
    # Row 3: RSI
    if 'rsi30' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['rsi30'],
                name='RSI(30)',
                line=dict(color='purple', width=2)
            ),
            row=3, col=1
        )
        
        # Add overbought/oversold lines
        fig.add_hline(y=70, line_dash="dash", line_color="red", 
                     annotation_text="Overbought", row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green",
                     annotation_text="Oversold", row=3, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="gray", row=3, col=1)
    
    # Row 4: CCI
    if 'cci30' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['cci30'],
                name='CCI(30)',
                line=dict(color='orange', width=2)
            ),
            row=4, col=1
        )
        
        # Add reference lines
        fig.add_hline(y=100, line_dash="dash", line_color="red", row=4, col=1)
        fig.add_hline(y=-100, line_dash="dash", line_color="green", row=4, col=1)
        fig.add_hline(y=0, line_dash="dot", line_color="gray", row=4, col=1)
    
    # Row 5: DX
    if 'dx30' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['dx30'],
                name='DX(30)',
                line=dict(color='cyan', width=2)
            ),
            row=5, col=1
        )
        
        # Add threshold lines
        fig.add_hline(y=25, line_dash="dash", line_color="yellow",
                     annotation_text="Strong Trend", row=5, col=1)
    
    # Update layout
    fig.update_layout(
        title=title,
        height=height,
        hovermode='x unified',
        template='plotly_dark',
        showlegend=True,
        xaxis_rangeslider_visible=False
    )
    
    # Update y-axis labels
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="MACD", row=2, col=1)
    fig.update_yaxes(title_text="RSI", row=3, col=1)
    fig.update_yaxes(title_text="CCI", row=4, col=1)
    fig.update_yaxes(title_text="DX", row=5, col=1)
    
    return fig


def create_correlation_heatmap(
    df: pd.DataFrame,
    title: Optional[str] = "Indicator Correlation Matrix",
    height: int = 600
) -> go.Figure:
    """
    Create correlation heatmap for indicators and price movements.
    
    Args:
        df: DataFrame with indicator columns
        title: Chart title
        height: Chart height in pixels
        
    Returns:
        Plotly heatmap figure
    """
    # Select numeric columns (indicators + price data)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    # Calculate correlation matrix
    corr_matrix = df[numeric_cols].corr()
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.columns,
        colorscale='RdBu',
        zmid=0,
        text=corr_matrix.values,
        texttemplate='%{text:.2f}',
        textfont={"size": 10},
        colorbar=dict(title="Correlation")
    ))
    
    fig.update_layout(
        title=title,
        height=height,
        template='plotly_dark'
    )
    
    return fig


def create_multi_ticker_comparison(
    dfs: dict,
    title: Optional[str] = "Multi-Ticker Price Comparison",
    normalize: bool = True,
    height: int = 600
) -> go.Figure:
    """
    Compare multiple tickers on the same chart.
    
    Args:
        dfs: Dictionary of {ticker: dataframe}
        title: Chart title
        normalize: Whether to normalize prices to % change from start
        height: Chart height in pixels
        
    Returns:
        Plotly line chart
    """
    fig = go.Figure()
    
    for ticker, df in dfs.items():
        if normalize:
            # Normalize to percentage change from first value
            first_price = df['close'].iloc[0]
            y_data = ((df['close'] - first_price) / first_price) * 100
            y_label = "% Change"
        else:
            y_data = df['close']
            y_label = "Price ($)"
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=y_data,
            name=ticker,
            mode='lines',
            line=dict(width=2)
        ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=y_label,
        height=height,
        hovermode='x unified',
        template='plotly_dark',
        showlegend=True
    )
    
    return fig


def create_returns_distribution(
    df: pd.DataFrame,
    ticker: str,
    title: Optional[str] = None,
    height: int = 500
) -> go.Figure:
    """
    Create histogram of daily returns distribution.
    
    Args:
        df: DataFrame with close prices
        ticker: Stock ticker symbol
        title: Chart title
        height: Chart height in pixels
        
    Returns:
        Plotly histogram figure
    """
    if title is None:
        title = f"{ticker} Daily Returns Distribution"
    
    # Calculate daily returns
    returns = df['close'].pct_change() * 100  # Convert to percentage
    
    fig = go.Figure(data=[go.Histogram(
        x=returns.dropna(),
        nbinsx=50,
        name='Returns',
        marker_color='#1f77b4',
        opacity=0.7
    )])
    
    # Add mean line
    mean_return = returns.mean()
    fig.add_vline(
        x=mean_return,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Mean: {mean_return:.2f}%"
    )
    
    fig.update_layout(
        title=title,
        xaxis_title="Daily Return (%)",
        yaxis_title="Frequency",
        height=height,
        template='plotly_dark',
        showlegend=False
    )
    
    return fig


# Example usage function
if __name__ == "__main__":
    # This would typically load data from MinIO or PostgreSQL
    print("Visualization module loaded. Import functions to use.")
    print("\nAvailable functions:")
    print("  - create_candlestick_chart()")
    print("  - create_technical_indicators_dashboard()")
    print("  - create_correlation_heatmap()")
    print("  - create_multi_ticker_comparison()")
    print("  - create_returns_distribution()")

