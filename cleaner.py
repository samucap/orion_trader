import os, glob, io
import numpy as np
import pandas as pd
import pyarrow as pa
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import talib.abstract as ta
from typing import Optional, Dict, Any
try:
    import psycopg2
    from psycopg2.extras import execute_values
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("psycopg2 not available - PostgreSQL support disabled")
load_dotenv()

# ANSI color codes for colored printing
class Colors:
    GREEN = '\033[92m'  # Success/OK
    YELLOW = '\033[93m'  # Warning
    RED = '\033[91m'  # Error
    BLUE = '\033[94m'  # Info
    CYAN = '\033[96m'  # Info
    RESET = '\033[0m'  # Reset to default

def print_success(message):
    """Print success message in green"""
    print(f"{Colors.GREEN}{message}{Colors.RESET}")

def print_warning(message):
    """Print warning message in yellow"""
    print(f"{Colors.YELLOW}{message}{Colors.RESET}")

def print_error(message):
    """Print error message in red"""
    print(f"{Colors.RED}{message}{Colors.RESET}")

def print_info(message):
    """Print info message in blue"""
    print(f"{Colors.BLUE}{message}{Colors.RESET}")

def process_year_files(year, storage_config=None, minio=None, lastclose=None, lasthi=None, lastlo=None):
    """
    Process all files for a given year and return row count statistics.
    
    Args:
        year: Year to process
        storage_config: Dict with storage backend configuration (optional, for new interface)
        minio: MinIO client (optional, for backward compatibility)
        lastclose, lasthi, lastlo: KV cache dictionaries
        
    Returns:
        Dictionary with processing statistics
    """
    files = sorted(glob.glob(f'./flatfiles/{year}/*/*'))
    print(f'Processing {len(files)} files for year {year}')

    # Initialize state dictionaries if not provided
    if lastclose is None:
        lastclose = {}
    if lasthi is None:
        lasthi = {}
    if lastlo is None:
        lastlo = {}

    # Track row counts for testing
    stats = {
        'files_processed': len(files),
        'total_csv_rows': 0,
        'tickers_processed': 0,
        'year_results_count': 0,
        'final_df_rows': 0
    }

    # Accumulate data for current year only (memory efficient)
    year_ticker_data = {}

    # Process files ONE AT A TIME
    for f in files:
        print(f'  Reading {f}')
        df = pd.read_csv(f, compression='gzip')
        stats['total_csv_rows'] += len(df)

        # Efficient date conversion
        df['date'] = pd.to_datetime(df['window_start'], utc=True).dt.strftime("%Y-%m-%d")

        # Group by ticker for this year
        for tic, ticdata in df.groupby('ticker'):
            if tic not in year_ticker_data:
                year_ticker_data[tic] = []
            year_ticker_data[tic].append(ticdata)

    # Process each ticker for this year
    year_results = []
    for tic, tic_dfs in year_ticker_data.items():
        # Combine ticker's data for this year
        tic_year_data = pd.concat(tic_dfs, ignore_index=True).sort_values('date')

        # Calculate indicators with historical context
        processed_tic = add_technical_indicators(tic, tic_year_data, lastclose, lasthi, lastlo)
        if processed_tic is not None:  # Skip if data integrity issues
            year_results.append(processed_tic)

    stats['tickers_processed'] = len(year_ticker_data)
    stats['year_results_count'] = len(year_results)

    # Write the year's data
    if year_results:
        year_df = pd.concat(year_results, ignore_index=True)
        # Sort by date, then ticker for consistent ordering
        year_df = year_df.sort_values(['date', 'ticker']).reset_index(drop=True)
        stats['final_df_rows'] = len(year_df)
        
        # Use new storage routing if config provided, otherwise fall back to legacy MinIO
        if storage_config:
            write_results = choose_storage_backend(year, year_df, storage_config, stats)
            print_info(f"Storage results: {write_results}")
        elif minio:
            # Backward compatibility: direct MinIO write
            write_to_minio(year_df, year, minio)
        
        print_success(f"Processed {year}.parquet with {len(year_df)} rows")
    else:
        print_warning(f"No data to write for year {year}")

    # Validate row counts
    validation_errors = validate_row_counts(stats)
    if validation_errors:
        print_warning(f"Validation errors for year {year}:")
        for error in validation_errors:
            print(f"  - {error}")
    else:
        print_success(f"Row count validation passed for year {year}")

    return stats

def runner(minio=None, process_all_years=False):
    lastclose = {}
    lasthi = {}
    lastlo = {}

    for year in range(2020, 2026):
        stats = process_year_files(year, minio, lastclose, lasthi, lastlo)
        print(f"Year {year} stats: {stats}")

        if not process_all_years:
            break
    # reading
    resp = minio.get_object('us-stock-day-aggs-v1', "2020.parquet")
    data = io.BytesIO(resp.read())
    pd.set_option('display.max_columns', None)
    newDF = pd.read_parquet(data, engine="pyarrow")
    print('>>>>>>>>>>>>> describe')
    newDF.describe()
    print(f'>>>>>>>>>>>>> info ', newDF.info())
    print(newDF.head(10))
    print(newDF.tail(10))
    print('dun')

    #resp = minio.get_object('us-stock-day-aggs-v1', '2021.parquet')
    #data = io.BytesIO(resp.read())
    #newDF = pd.read_parquet(data)
    #print('checking >>>>>>>>>>>>> ', newDF.info())
    #print(newDF.head())
    #print(newDF.tail())

def calculate_indicators_for_series(close, hi, lo):
    """
    Calculate technical indicators for given price series.
    
    Pure calculation function without cache management - easier to test.
    
    Args:
        close: pandas Series of close prices
        hi: pandas Series of high prices  
        lo: pandas Series of low prices
        
    Returns:
        DataFrame with calculated indicators
    """
    macd, _, _ = ta.MACD(close)
    bollub, _, bolllb = ta.BBANDS(close)
    rsi30 = ta.RSI(close, timeperiod=30)
    sma30 = ta.SMA(close, timeperiod=30)
    sma60 = ta.SMA(close, timeperiod=60)
    cci30 = ta.CCI(hi, lo, close, timeperiod=30)
    dx30 = ta.DX(hi, lo, close, timeperiod=30)
    
    return pd.DataFrame({
        'macd': macd,
        'bollub': bollub,
        'bolllb': bolllb,
        'rsi30': rsi30,
        'sma30': sma30,
        'sma60': sma60,
        'cci30': cci30,
        'dx30': dx30
    })

def add_technical_indicators(tic, df: pd.DataFrame, lastclose, lasthi, lastlo):
    # Sort current year's data chronologically
    df = df.sort_values('date')

    close = df['close']
    hi = df['high']
    lo = df['low']

    # Prepend historical data
    hist_close = lastclose.get(tic)
    hist_hi = lasthi.get(tic)
    hist_lo = lastlo.get(tic)


    if hist_close is not None:
        close = pd.concat([hist_close, close])
    if hist_hi is not None:
        hi = pd.concat([hist_hi, hi])
    if hist_lo is not None:
        lo = pd.concat([hist_lo, lo])

    # Check for data integrity issues
    lengths = [len(close), len(hi), len(lo)]
    max_len = max(lengths)
    min_len = min(lengths)

    # If length difference is > 10%, there's likely a data issue
    if max_len > min_len * 1.1:  # 10% tolerance
        print_warning(f"Data integrity issue for {tic}: lengths {lengths}")
        print(f"Skipping {tic} for this year due to data mismatch")
        return None  # Skip this ticker

    # BETTER: Ensure all series have same length using proper interpolation
    target_len = max_len

    # Function to safely align series lengths
    def align_series(series, target_len, series_name):
        if len(series) == target_len:
            return series.reset_index(drop=True)

        # If series is shorter, use linear interpolation to extend
        if len(series) < target_len:
            # Create index for interpolation
            original_idx = np.arange(len(series))
            target_idx = np.linspace(0, len(series)-1, target_len)

            # Interpolate values
            interpolated = np.interp(target_idx, original_idx, series.values)
            return pd.Series(interpolated).reset_index(drop=True)

        # If series is longer (shouldn't happen with our checks), truncate
        return series.iloc[:target_len].reset_index(drop=True)

    # Align all series to same length
    close = align_series(close, target_len, 'close')
    hi = align_series(hi, target_len, 'high')
    lo = align_series(lo, target_len, 'low')

    # Verify alignment
    if not (len(close) == len(hi) == len(lo)):
        print_error(f"Failed to align series for {tic}")
        return None

    # Calculate indicators on properly aligned data using extracted function
    indicators_df = calculate_indicators_for_series(close, hi, lo)

    # Extract current year's portion
    current_year_len = len(df)
    historical_len = target_len - current_year_len

    if historical_len > 0:
        current_indicators = indicators_df.iloc[historical_len:].copy()
    else:
        current_indicators = indicators_df.copy()

    # Handle NaN values from lookback periods (industry standard approach)
    # 1. Forward fill to handle gaps in the middle of the data
    # 2. Leave NaN at the beginning where lookback data is insufficient
    # 3. Avoid backward fill as it would use future data (not available at processing time)
    current_indicators = current_indicators.ffill()

    # Update state with most recent data (ensure consistency)
    # KV Cache Memory Bounds:
    # - Cache stores last 60 rows per ticker (matches SMA60, the largest lookback period)
    # - Estimated memory: ~10,000 tickers × 60 rows × 3 series (close/hi/lo) × 8 bytes ≈ 14.4 MB
    # - This is a small, bounded cache that prevents memory issues even with many tickers
    state_len = min(60, len(close))
    if state_len >= 30:  # Minimum threshold for reliable indicators
        lastclose[tic] = close.iloc[-state_len:]
        lasthi[tic] = hi.iloc[-state_len:]
        lastlo[tic] = lo.iloc[-state_len:]

    # Add indicators to current year's dataframe
    result_df = df.copy()
    for col in current_indicators.columns:
        result_df[col] = current_indicators[col].values

    return result_df

def write_to_minio(df, year, minio):
    """Write DataFrame to MinIO as parquet file"""
    if minio is None:
        print_warning(f"MinIO client not provided, skipping upload for {year}.parquet")
        return None

    bstream = io.BytesIO()
    bstream.write(df.to_parquet(engine="pyarrow"))
    nbytes = bstream.getbuffer().nbytes
    print(f'Uploading {year}.parquet - {nbytes/1000000:.2f} MB')

    bstream.seek(0)
    result = minio.put_object(
        "us-stock-day-aggs-v1",
        f'{year}.parquet',
        bstream,
        nbytes,
        content_type="application/octet-stream"
    )
    print_success(f"Success: {result.__dict__}")
    bstream.close()
    return result

def write_to_postgres(df: pd.DataFrame, year: int, conn_string: Optional[str] = None, stats: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
    """
    Write DataFrame to PostgreSQL with bulk insert optimization.
    
    Args:
        df: DataFrame with processed stock data
        year: Year being processed
        conn_string: PostgreSQL connection string (defaults to env var)
        stats: Optional processing statistics to log
        
    Returns:
        Dictionary with write results or None on failure
    """
    if not POSTGRES_AVAILABLE:
        print_warning("psycopg2 not installed - cannot write to PostgreSQL")
        return None
        
    if conn_string is None:
        conn_string = os.getenv('POSTGRES_CONN')
        if not conn_string:
            print_warning("POSTGRES_CONN not set, skipping PostgreSQL write")
            return None
    
    # Prepare data for bulk insert
    columns = ['ticker', 'date', 'window_start', 'open', 'high', 'low', 
               'close', 'volume', 'transactions', 'macd', 'bollub', 
               'bolllb', 'rsi30', 'sma30', 'sma60', 'cci30', 'dx30']
    
    # Ensure window_start is timestamp
    if 'window_start' in df.columns:
        if pd.api.types.is_integer_dtype(df['window_start']):
            df = df.copy()
            df['window_start'] = pd.to_datetime(df['window_start'], unit='ns', utc=True)
    
    # Prepare tuples for insert (replace NaN with None for SQL)
    df_subset = df[columns].copy()
    df_subset = df_subset.where(pd.notnull(df_subset), None)
    records = [tuple(row) for row in df_subset.values]
    
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        # Bulk insert using execute_values (fast)
        insert_query = f"""
            INSERT INTO stock_prices ({','.join(columns)})
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                transactions = EXCLUDED.transactions,
                macd = EXCLUDED.macd,
                bollub = EXCLUDED.bollub,
                bolllb = EXCLUDED.bolllb,
                rsi30 = EXCLUDED.rsi30,
                sma30 = EXCLUDED.sma30,
                sma60 = EXCLUDED.sma60,
                cci30 = EXCLUDED.cci30,
                dx30 = EXCLUDED.dx30
        """
        
        execute_values(cursor, insert_query, records, page_size=1000)
        
        # Log processing run if stats provided
        if stats:
            cursor.execute("""
                INSERT INTO processing_runs (year, files_processed, total_csv_rows, 
                                            tickers_processed, final_df_rows, success)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (year, 
                  stats.get('files_processed', 0), 
                  stats.get('total_csv_rows', 0),
                  stats.get('tickers_processed', 0), 
                  stats.get('final_df_rows', 0),
                  True))
        
        conn.commit()
        print_success(f"Written {len(df)} rows to PostgreSQL for year {year}")
        
        cursor.close()
        conn.close()
        
        return {'rows_written': len(df), 'year': year, 'success': True}
        
    except Exception as e:
        print_error(f"PostgreSQL write failed: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return {'success': False, 'error': str(e)}

def choose_storage_backend(year: int, df: pd.DataFrame, config: Dict[str, Any], stats: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Route data to appropriate storage backends based on configuration.
    
    Args:
        year: Year being processed
        df: DataFrame to write
        config: Configuration dict with storage backend settings
        stats: Optional processing statistics
        
    Returns:
        Dictionary with results from each backend
    """
    backends = config.get('storage_backends', ['minio'])
    results = {}
    
    if 'minio' in backends:
        minio = config.get('minio_client')
        if minio:
            try:
                results['minio'] = write_to_minio(df, year, minio)
            except Exception as e:
                print_error(f"MinIO write failed: {e}")
                results['minio'] = {'success': False, 'error': str(e)}
        else:
            print_warning("MinIO client not provided in config")
            results['minio'] = None
    
    if 'postgres' in backends:
        postgres_conn = config.get('postgres_conn')
        if postgres_conn or os.getenv('POSTGRES_CONN'):
            try:
                results['postgres'] = write_to_postgres(df, year, postgres_conn, stats)
            except Exception as e:
                print_error(f"PostgreSQL write failed: {e}")
                results['postgres'] = {'success': False, 'error': str(e)}
        else:
            print_warning("PostgreSQL connection not configured")
            results['postgres'] = None
    
    return results

def validate_row_counts(stats):
    """Validate that row counts are consistent throughout processing"""
    errors = []

    # Check that we have processed some data
    if stats['total_csv_rows'] == 0:
        errors.append("No CSV rows were read")

    if stats['tickers_processed'] == 0:
        errors.append("No tickers were processed")

    # Check that year_results count matches tickers processed
    # (allowing for some tickers to be skipped due to data issues)
    if stats['year_results_count'] > stats['tickers_processed']:
        errors.append(f"year_results_count ({stats['year_results_count']}) > tickers_processed ({stats['tickers_processed']})")

    # Check that final_df_rows matches sum of year_results
    if stats['year_results_count'] > 0 and stats['final_df_rows'] == 0:
        errors.append("Final DataFrame has 0 rows but year_results is not empty")

    return errors

def createBucket(client, bucketName):
    try:
        if not client.bucket_exists(bucketName):
            client.make_bucket(bucketName)
            print(f'Bucket {bucketName} created successfully.')
        else:
            print(f'Bucket {bucketName} already exists.')
    except S3Error as e:
        print(f'Error: {e}')

def makeMinIO():
    c = Minio(
        os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ROOT_USER', 'key'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'pw'),
        secure=False
    )

    createBucket(c, 'us-stock-day-aggs-v1')
    return c



if __name__ == '__main__':
    minio = makeMinIO()
    #TODO: review this
    isdev = True if os.getenv('ENV', 'false') == 'true' else False
    runner(minio, isdev)


