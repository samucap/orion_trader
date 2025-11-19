"""
RAG SQL Query Agent

Translates natural language queries into SQL and provides intelligent responses
about stock market data using LLM capabilities.
"""

import os
import json
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

# Try to import LLM libraries (gracefully handle if not installed)
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

load_dotenv()


class SQLQueryAgent:
    """
    Natural language to SQL query agent for stock market data.
    
    Supports both OpenAI API and local Ollama models.
    """
    
    def __init__(
        self,
        postgres_conn: Optional[str] = None,
        model: str = "gpt-4",
        use_ollama: bool = False,
        ollama_url: str = "http://localhost:11434"
    ):
        """
        Initialize SQL Query Agent.
        
        Args:
            postgres_conn: PostgreSQL connection string
            model: LLM model to use (gpt-4, gpt-3.5-turbo, or ollama model)
            use_ollama: Whether to use local Ollama instead of OpenAI
            ollama_url: Ollama API URL
        """
        self.postgres_conn = postgres_conn or os.getenv('POSTGRES_CONN')
        self.model = model
        self.use_ollama = use_ollama
        self.ollama_url = ollama_url
        
        if not POSTGRES_AVAILABLE:
            print("Warning: psycopg2 not installed - SQL execution disabled")
        
        if use_ollama:
            # Use requests for Ollama
            try:
                import requests
                self.requests = requests
            except ImportError:
                raise ImportError("requests library required for Ollama")
        else:
            if not OPENAI_AVAILABLE:
                raise ImportError("openai library required for OpenAI API")
            openai.api_key = os.getenv('OPENAI_API_KEY')
        
        # Database schema for context
        self.schema_context = """
        Database Schema:
        
        Table: stock_prices (partitioned by date)
        Columns:
          - ticker VARCHAR(10)
          - date DATE
          - window_start TIMESTAMPTZ
          - open DECIMAL(12,4)
          - high DECIMAL(12,4)
          - low DECIMAL(12,4)
          - close DECIMAL(12,4)
          - volume BIGINT
          - transactions INTEGER
          - macd DECIMAL(12,6)
          - bollub DECIMAL(12,4) -- Bollinger upper band
          - bolllb DECIMAL(12,4) -- Bollinger lower band
          - rsi30 DECIMAL(8,4) -- RSI with 30 period
          - sma30 DECIMAL(12,4) -- Simple Moving Average 30
          - sma60 DECIMAL(12,4) -- Simple Moving Average 60
          - cci30 DECIMAL(12,4) -- Commodity Channel Index 30
          - dx30 DECIMAL(12,4) -- Directional Movement Index 30
        
        Table: tickers
        Columns:
          - ticker VARCHAR(10)
          - company_name VARCHAR(255)
          - sector VARCHAR(100)
          - industry VARCHAR(100)
          - market_cap BIGINT
          - is_active BOOLEAN
        
        Table: price_anomalies
        Columns:
          - ticker VARCHAR(10)
          - date DATE
          - anomaly_type VARCHAR(50)
          - severity VARCHAR(20)
          - details JSONB
        
        View: stock_analysis (includes computed signals like 'overbought', 'oversold', etc.)
        """
    
    def generate_sql(self, natural_query: str) -> Dict[str, Any]:
        """
        Convert natural language query to SQL.
        
        Args:
            natural_query: User's question in natural language
            
        Returns:
            Dictionary with 'sql', 'explanation', and 'query_type'
        """
        prompt = f"""You are a SQL expert for a stock market database. Convert the following natural language query into a PostgreSQL SQL query.

{self.schema_context}

User Query: {natural_query}

Provide:
1. A valid PostgreSQL SQL query
2. A brief explanation of what the query does
3. The query type (SELECT, INSERT, UPDATE, DELETE)

Respond in JSON format:
{{
    "sql": "SELECT ...",
    "explanation": "This query...",
    "query_type": "SELECT"
}}

Important guidelines:
- Use proper JOIN syntax when needed
- Include appropriate WHERE clauses for filtering
- Use ORDER BY and LIMIT for large result sets
- For date ranges, use INTERVAL syntax (e.g., date >= CURRENT_DATE - INTERVAL '7 days')
- For technical indicators, remember: RSI > 70 is overbought, RSI < 30 is oversold
- MACD > 0 is bullish signal, MACD < 0 is bearish
- Use window functions (LAG, LEAD) for comparing consecutive values
"""
        
        if self.use_ollama:
            response = self._call_ollama(prompt)
        else:
            response = self._call_openai(prompt)
        
        try:
            result = json.loads(response)
            return result
        except json.JSONDecodeError:
            # Fallback: try to extract SQL from response
            return {
                "sql": response,
                "explanation": "Generated SQL query",
                "query_type": "SELECT"
            }
    
    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of result rows as dictionaries
        """
        if not POSTGRES_AVAILABLE:
            raise RuntimeError("psycopg2 not installed")
        
        if not self.postgres_conn:
            raise ValueError("PostgreSQL connection string not configured")
        
        conn = psycopg2.connect(self.postgres_conn)
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch results
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = [dict(zip(columns, row)) for row in rows]
            
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            cursor.close()
            conn.close()
            raise RuntimeError(f"SQL execution failed: {e}")
    
    def generate_response(
        self,
        natural_query: str,
        sql_results: List[Dict[str, Any]],
        sql_explanation: str
    ) -> str:
        """
        Generate natural language response from SQL results.
        
        Args:
            natural_query: Original user query
            sql_results: Results from SQL execution
            sql_explanation: Explanation of what the SQL does
            
        Returns:
            Natural language response
        """
        # Prepare results summary
        if not sql_results:
            results_text = "No results found."
        elif len(sql_results) > 10:
            results_text = f"Found {len(sql_results)} results. First 10:\n" + \
                          json.dumps(sql_results[:10], indent=2, default=str)
        else:
            results_text = json.dumps(sql_results, indent=2, default=str)
        
        prompt = f"""Based on the following query and results, provide a helpful natural language response.

User Question: {natural_query}

SQL Query Explanation: {sql_explanation}

Results:
{results_text}

Provide a concise, informative response that:
1. Directly answers the user's question
2. Highlights key insights from the data
3. Uses proper financial terminology
4. Mentions specific numbers and tickers when relevant

Response:"""
        
        if self.use_ollama:
            return self._call_ollama(prompt)
        else:
            return self._call_openai(prompt)
    
    def query(self, natural_query: str, execute: bool = True) -> Dict[str, Any]:
        """
        Full RAG pipeline: parse query, execute SQL, generate response.
        
        Args:
            natural_query: User's question in natural language
            execute: Whether to execute the SQL (False for dry-run)
            
        Returns:
            Dictionary with 'sql', 'results', 'response', 'explanation'
        """
        # Step 1: Generate SQL
        sql_data = self.generate_sql(natural_query)
        
        result = {
            'query': natural_query,
            'sql': sql_data.get('sql'),
            'explanation': sql_data.get('explanation'),
            'query_type': sql_data.get('query_type')
        }
        
        if not execute:
            result['response'] = "SQL generated but not executed (dry-run mode)"
            return result
        
        # Step 2: Execute SQL
        try:
            results = self.execute_query(sql_data['sql'])
            result['results'] = results
            result['result_count'] = len(results)
        except Exception as e:
            result['error'] = str(e)
            result['results'] = []
            result['result_count'] = 0
            result['response'] = f"Error executing query: {e}"
            return result
        
        # Step 3: Generate natural language response
        try:
            response = self.generate_response(
                natural_query,
                results,
                sql_data.get('explanation', '')
            )
            result['response'] = response
        except Exception as e:
            result['response'] = f"Results retrieved but response generation failed: {e}"
        
        return result
    
    def _call_openai(self, prompt: str) -> str:
        """Call OpenAI API."""
        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a SQL and financial data expert."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1  # Low temperature for more consistent SQL generation
        )
        return response.choices[0].message.content
    
    def _call_ollama(self, prompt: str) -> str:
        """Call local Ollama API."""
        response = self.requests.post(
            f"{self.ollama_url}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False
            }
        )
        response.raise_for_status()
        return response.json()['response']


# Example usage
if __name__ == "__main__":
    print("RAG SQL Agent initialized.")
    print("\nExample queries:")
    print("  - 'Show me all tickers where RSI dropped below 30 in the last week'")
    print("  - 'Which tech stocks have bullish MACD crossovers?'")
    print("  - 'Find stocks with the highest volume yesterday'")
    print("  - 'Show price anomalies for AAPL in Q4 2024'")
    print("\nTo use:")
    print("  agent = SQLQueryAgent(postgres_conn='your_connection_string')")
    print("  result = agent.query('your natural language question')")

