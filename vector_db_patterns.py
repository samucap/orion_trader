"""
Vector Database for Stock Market Pattern Storage and Retrieval

Stores embeddings of price patterns, technical indicator combinations,
and market narratives for similarity search and pattern recognition.
"""

import os
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import json

# Try to import vector database libraries
try:
    import chromadb
    from chromadb.config import Settings
    CHROMA_AVAILABLE = True
except ImportError:
    CHROMA_AVAILABLE = False
    print("chromadb not installed - vector database disabled")

# Try to import sentence transformers for embeddings
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    print("sentence-transformers not installed - using fallback embeddings")


class StockPatternVectorDB:
    """
    Vector database for storing and retrieving stock market patterns.
    
    Uses Chroma DB for vector storage and sentence transformers for embeddings.
    """
    
    def __init__(
        self,
        persist_directory: str = "./chroma_db",
        collection_name: str = "stock_patterns",
        embedding_model: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize vector database.
        
        Args:
            persist_directory: Directory to persist the database
            collection_name: Name of the collection
            embedding_model: Sentence transformer model name
        """
        if not CHROMA_AVAILABLE:
            raise ImportError("chromadb is required. Install: pip install chromadb")
        
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # Initialize Chroma client
        self.client = chromadb.Client(Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=persist_directory
        ))
        
        # Get or create collection
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Stock market price and indicator patterns"}
        )
        
        # Initialize embedding model
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            self.embedding_model = SentenceTransformer(embedding_model)
        else:
            self.embedding_model = None
            print("Warning: Using random embeddings as fallback")
    
    def embed_text(self, text: str) -> List[float]:
        """
        Generate embedding for text.
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector
        """
        if self.embedding_model:
            return self.embedding_model.encode(text).tolist()
        else:
            # Fallback: random embedding (not useful for real use)
            np.random.seed(hash(text) % (2**32))
            return np.random.randn(384).tolist()
    
    def create_pattern_description(
        self,
        ticker: str,
        date: str,
        price_data: Dict[str, float],
        indicators: Dict[str, float],
        pattern_type: Optional[str] = None,
        outcome: Optional[str] = None
    ) -> str:
        """
        Create textual description of a market pattern.
        
        Args:
            ticker: Stock ticker
            date: Date of pattern
            price_data: Dict with open, high, low, close, volume
            indicators: Dict with technical indicators
            pattern_type: Type of pattern (optional)
            outcome: What happened after (optional)
            
        Returns:
            Text description suitable for embedding
        """
        parts = [f"Stock: {ticker}", f"Date: {date}"]
        
        # Price movement
        if 'open' in price_data and 'close' in price_data:
            change = ((price_data['close'] - price_data['open']) / price_data['open']) * 100
            direction = "up" if change > 0 else "down"
            parts.append(f"Price moved {direction} {abs(change):.2f}%")
        
        # Technical indicators
        if 'rsi30' in indicators:
            rsi = indicators['rsi30']
            if rsi > 70:
                parts.append(f"RSI overbought at {rsi:.1f}")
            elif rsi < 30:
                parts.append(f"RSI oversold at {rsi:.1f}")
            else:
                parts.append(f"RSI neutral at {rsi:.1f}")
        
        if 'macd' in indicators:
            macd = indicators['macd']
            signal = "bullish" if macd > 0 else "bearish"
            parts.append(f"MACD {signal} signal at {macd:.2f}")
        
        if 'bollinger_position' in indicators:
            parts.append(f"Bollinger Bands: {indicators['bollinger_position']}")
        
        if pattern_type:
            parts.append(f"Pattern: {pattern_type}")
        
        if outcome:
            parts.append(f"Outcome: {outcome}")
        
        return ". ".join(parts) + "."
    
    def add_pattern(
        self,
        pattern_id: str,
        ticker: str,
        date: str,
        price_data: Dict[str, float],
        indicators: Dict[str, float],
        pattern_type: Optional[str] = None,
        outcome: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Add a pattern to the vector database.
        
        Args:
            pattern_id: Unique identifier for pattern
            ticker: Stock ticker
            date: Date of pattern
            price_data: Price OHLCV data
            indicators: Technical indicator values
            pattern_type: Type of pattern (e.g., 'bullish_reversal')
            outcome: What happened after (e.g., '7_day_gain_12pct')
            metadata: Additional metadata
            
        Returns:
            Pattern ID
        """
        # Create text description
        description = self.create_pattern_description(
            ticker, date, price_data, indicators, pattern_type, outcome
        )
        
        # Generate embedding
        embedding = self.embed_text(description)
        
        # Prepare metadata
        full_metadata = {
            "ticker": ticker,
            "date": date,
            "pattern_type": pattern_type or "unknown",
            "outcome": outcome or "unknown",
            **price_data,
            **indicators,
            **(metadata or {})
        }
        
        # Convert all metadata values to strings (Chroma requirement)
        full_metadata = {k: str(v) if v is not None else "null" 
                        for k, v in full_metadata.items()}
        
        # Add to collection
        self.collection.add(
            ids=[pattern_id],
            embeddings=[embedding],
            documents=[description],
            metadatas=[full_metadata]
        )
        
        return pattern_id
    
    def search_similar_patterns(
        self,
        query_text: str,
        n_results: int = 5,
        filter_metadata: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar patterns using text query.
        
        Args:
            query_text: Natural language description of pattern to find
            n_results: Number of results to return
            filter_metadata: Optional metadata filters
            
        Returns:
            List of matching patterns with scores
        """
        # Generate query embedding
        query_embedding = self.embed_text(query_text)
        
        # Search
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=filter_metadata
        )
        
        # Format results
        formatted_results = []
        if results['ids'] and len(results['ids']) > 0:
            for i, pattern_id in enumerate(results['ids'][0]):
                formatted_results.append({
                    'id': pattern_id,
                    'distance': results['distances'][0][i],
                    'document': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i]
                })
        
        return formatted_results
    
    def search_by_indicators(
        self,
        indicators: Dict[str, float],
        ticker: Optional[str] = None,
        n_results: int = 5,
        tolerance: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Search for patterns with similar indicator values.
        
        Args:
            indicators: Dict of indicator values to match
            ticker: Optional ticker filter
            n_results: Number of results
            tolerance: Tolerance for numeric matching (0.1 = 10%)
            
        Returns:
            List of matching patterns
        """
        # Create query from indicators
        query_parts = []
        for name, value in indicators.items():
            query_parts.append(f"{name} around {value}")
        
        if ticker:
            query_parts.append(f"stock {ticker}")
        
        query_text = " ".join(query_parts)
        
        # Search
        return self.search_similar_patterns(query_text, n_results)
    
    def bulk_add_from_dataframe(
        self,
        df: pd.DataFrame,
        pattern_type: str = "historical",
        calculate_outcomes: bool = True,
        outcome_window: int = 7
    ) -> int:
        """
        Add multiple patterns from a DataFrame.
        
        Args:
            df: DataFrame with stock data
            pattern_type: Type to assign to all patterns
            calculate_outcomes: Whether to calculate future outcomes
            outcome_window: Days forward to look for outcome
            
        Returns:
            Number of patterns added
        """
        count = 0
        df = df.sort_values('date')
        
        for idx, row in df.iterrows():
            # Extract price data
            price_data = {
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume'])
            }
            
            # Extract indicators
            indicators = {}
            for col in ['macd', 'rsi30', 'sma30', 'sma60', 'cci30', 'dx30', 
                       'bollub', 'bolllb']:
                if col in row and pd.notna(row[col]):
                    indicators[col] = float(row[col])
            
            # Calculate outcome if requested
            outcome = None
            if calculate_outcomes and idx < len(df) - outcome_window:
                future_close = df.iloc[idx + outcome_window]['close']
                change = ((future_close - row['close']) / row['close']) * 100
                outcome = f"{outcome_window}d_{'gain' if change > 0 else 'loss'}_{abs(change):.1f}pct"
            
            # Create pattern ID
            pattern_id = f"{row['ticker']}_{row['date']}_{idx}"
            
            try:
                self.add_pattern(
                    pattern_id=pattern_id,
                    ticker=row['ticker'],
                    date=str(row['date']),
                    price_data=price_data,
                    indicators=indicators,
                    pattern_type=pattern_type,
                    outcome=outcome
                )
                count += 1
            except Exception as e:
                print(f"Failed to add pattern {pattern_id}: {e}")
        
        return count
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics about the collection."""
        count = self.collection.count()
        return {
            'total_patterns': count,
            'collection_name': self.collection_name,
            'persist_directory': self.persist_directory
        }
    
    def persist(self):
        """Persist the database to disk."""
        self.client.persist()


# Example usage and helper functions
def create_pattern_from_row(row: pd.Series) -> Dict[str, Any]:
    """Helper to convert DataFrame row to pattern dict."""
    return {
        'ticker': row['ticker'],
        'date': str(row['date']),
        'price_data': {
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': int(row['volume'])
        },
        'indicators': {
            'macd': float(row.get('macd', 0)),
            'rsi30': float(row.get('rsi30', 50)),
            'sma30': float(row.get('sma30', 0)),
            'sma60': float(row.get('sma60', 0)),
        }
    }


if __name__ == "__main__":
    print("Vector Database for Stock Patterns initialized.")
    print("\nUsage:")
    print("  db = StockPatternVectorDB()")
    print("  db.add_pattern(...)")
    print("  results = db.search_similar_patterns('bullish reversal with low RSI')")
    print("\nFeatures:")
    print("  - Store price patterns with embeddings")
    print("  - Search by natural language descriptions")
    print("  - Find similar indicator combinations")
    print("  - Track pattern outcomes for backtesting")

