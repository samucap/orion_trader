"""
Example Usage Script - Stock Trader Enhanced Features

Demonstrates how to use all the new features together:
1. Hybrid storage (MinIO + PostgreSQL)
2. Visualizations
3. RAG SQL queries
4. Vector pattern search
"""

import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("Stock Trader - Enhanced Features Demo")
print("=" * 60)

# ============================================================================
# 1. HYBRID STORAGE EXAMPLE
# ============================================================================
print("\n[1] Hybrid Storage Example")
print("-" * 60)

try:
    from cleaner import makeMinIO, process_year_files
    
    # Setup storage configuration
    minio_client = makeMinIO()
    
    storage_config = {
        'storage_backends': ['minio', 'postgres'],  # Both backends
        'minio_client': minio_client,
        'postgres_conn': os.getenv('POSTGRES_CONN')
    }
    
    print("Storage config:", storage_config['storage_backends'])
    print("Note: Uncomment the following to actually process data:")
    print("  stats = process_year_files(2020, storage_config=storage_config)")
    print("  This will write to both MinIO and PostgreSQL")
    
except Exception as e:
    print(f"Storage example skipped: {e}")

# ============================================================================
# 2. VISUALIZATIONS EXAMPLE
# ============================================================================
print("\n[2] Visualizations Example")
print("-" * 60)

try:
    from visualizations import (
        create_candlestick_chart,
        create_technical_indicators_dashboard,
        create_correlation_heatmap,
        create_multi_ticker_comparison
    )
    
    # Check if we have test data
    test_file = 'test_data/AAPL_2020_full.csv'
    if os.path.exists(test_file):
        print(f"Loading test data from {test_file}...")
        df = pd.read_csv(test_file)
        df['date'] = pd.to_datetime(df['window_start'], unit='ns', utc=True).dt.date
        
        print(f"Loaded {len(df)} rows for AAPL")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Create candlestick chart
        print("\nCreating candlestick chart...")
        fig = create_candlestick_chart(
            df=df,
            ticker='AAPL',
            show_volume=True,
            show_sma=False  # Test data may not have SMAs
        )
        output_file = 'aapl_candlestick.html'
        fig.write_html(output_file)
        print(f"✓ Saved interactive chart to {output_file}")
        
        # Create returns distribution
        from visualizations import create_returns_distribution
        fig2 = create_returns_distribution(df, 'AAPL')
        fig2.write_html('aapl_returns.html')
        print("✓ Saved returns distribution to aapl_returns.html")
        
    else:
        print(f"Test data not found at {test_file}")
        print("To create visualizations:")
        print("  1. Load your parquet data")
        print("  2. Call create_candlestick_chart(df, ticker='AAPL')")
        print("  3. Call fig.show() or fig.write_html('chart.html')")
    
except Exception as e:
    print(f"Visualization example error: {e}")

# ============================================================================
# 3. RAG SQL AGENT EXAMPLE
# ============================================================================
print("\n[3] RAG SQL Agent Example")
print("-" * 60)

try:
    from rag_sql_agent import SQLQueryAgent
    
    postgres_conn = os.getenv('POSTGRES_CONN')
    
    if postgres_conn:
        print("PostgreSQL connection configured")
        print("Note: Requires OpenAI API key or Ollama for LLM")
        
        # Example queries (won't execute without LLM setup)
        example_queries = [
            "Show me all tickers where RSI dropped below 30 in the last week",
            "Which stocks have bullish MACD crossovers?",
            "Find stocks with highest volume yesterday",
            "Show price anomalies for AAPL in December"
        ]
        
        print("\nExample natural language queries:")
        for i, query in enumerate(example_queries, 1):
            print(f"  {i}. {query}")
        
        print("\nTo use RAG agent:")
        print("  agent = SQLQueryAgent(postgres_conn='...')")
        print("  result = agent.query('your question here')")
        print("  print(result['response'])")
        
    else:
        print("PostgreSQL not configured (set POSTGRES_CONN env var)")
        print("RAG agent requires both PostgreSQL and an LLM (OpenAI/Ollama)")
    
except Exception as e:
    print(f"RAG agent example skipped: {e}")

# ============================================================================
# 4. VECTOR PATTERN SEARCH EXAMPLE  
# ============================================================================
print("\n[4] Vector Pattern Search Example")
print("-" * 60)

try:
    from vector_db_patterns import StockPatternVectorDB
    
    # This example shows the API but won't create actual embeddings
    # without sentence-transformers installed
    
    print("Vector DB for pattern similarity search")
    print("\nExample workflow:")
    print("  1. db = StockPatternVectorDB()")
    print("  2. db.bulk_add_from_dataframe(df)")
    print("  3. results = db.search_similar_patterns('bullish reversal')")
    print("  4. Examine similar historical patterns with outcomes")
    
    print("\nUse cases:")
    print("  - Find similar price patterns")
    print("  - Identify indicator combinations")
    print("  - Track pattern outcomes for backtesting")
    print("  - Anomaly detection via dissimilarity")
    
    print("\nRequires: chromadb, sentence-transformers")
    
except Exception as e:
    print(f"Vector DB example skipped: {e}")

# ============================================================================
# 5. INTEGRATION EXAMPLE
# ============================================================================
print("\n[5] Complete Integration Example")
print("-" * 60)

print("""
Complete workflow:

Step 1: Process Data
-------------------
from cleaner import process_year_files, makeMinIO

storage_config = {
    'storage_backends': ['minio', 'postgres'],
    'minio_client': makeMinIO(),
    'postgres_conn': 'postgresql://...'
}

stats = process_year_files(2020, storage_config=storage_config)
# Data now in both MinIO (parquet) and PostgreSQL (queryable)

Step 2: Create Visualizations
------------------------------
from visualizations import create_candlestick_chart
import pandas as pd

df = pd.read_parquet('2020.parquet')
aapl_data = df[df['ticker'] == 'AAPL']

fig = create_candlestick_chart(aapl_data, 'AAPL')
fig.write_html('aapl_analysis.html')
# Interactive chart with SMAs and volume

Step 3: Query with Natural Language
-----------------------------------
from rag_sql_agent import SQLQueryAgent

agent = SQLQueryAgent(postgres_conn='postgresql://...')
result = agent.query("Show tech stocks with oversold RSI")

print(result['sql'])        # Generated SQL query
print(result['results'])    # Query results
print(result['response'])   # Natural language answer

Step 4: Find Similar Patterns
-----------------------------
from vector_db_patterns import StockPatternVectorDB

db = StockPatternVectorDB()
db.bulk_add_from_dataframe(aapl_data, calculate_outcomes=True)

similar = db.search_similar_patterns(
    "bullish MACD crossover with low RSI"
)

for match in similar:
    print(f"Pattern: {match['document']}")
    print(f"Outcome: {match['metadata']['outcome']}")
    # See what happened after similar patterns
""")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 60)
print("Summary")
print("=" * 60)
print("""
All features implemented and ready to use:

✓ Hybrid Storage (MinIO + PostgreSQL)
✓ Interactive Visualizations (Plotly)
✓ RAG SQL Agent (Natural Language Queries)
✓ Vector Pattern Search (Similarity Matching)

See FEATURES_README.md for complete documentation.
See schema.sql for PostgreSQL setup.
See requirements.txt for dependencies.
""")

print("\nTo get started:")
print("1. pip install -r requirements.txt")
print("2. Setup PostgreSQL: psql -d trader -f schema.sql")
print("3. Set environment variables in .env")
print("4. Run this script: python example_usage.py")

if __name__ == "__main__":
    print("\nExample script completed!")

