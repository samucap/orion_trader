import unittest
import tempfile
import os
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
import io
import talib.abstract as ta

# Add the current directory to the path so we can import cleaner
sys.path.insert(0, os.path.dirname(__file__))

from cleaner import (
    process_year_files,
    validate_row_counts,
    add_technical_indicators,
    write_to_minio,
    print_success,
    print_warning,
    print_error,
    print_info,
    calculate_indicators_for_series
)

class TestCleanerRealData(unittest.TestCase):
    """Tests using actual CSV files from flatfiles directory"""

    # Configurable year for testing (defaults to 2020)
    TEST_YEAR = int(os.environ.get('TEST_YEAR', '2020'))

    def setUp(self):
        """Set up for real data tests"""
        self.original_cwd = os.getcwd()
        # Change to project root to access flatfiles
        os.chdir('/Users/0x/code/trader')

    def tearDown(self):
        """Clean up after real data tests"""
        os.chdir(self.original_cwd)

    def get_expected_row_count(self, year=None, use_half_files=False):
        """Get the expected total row count for CSV files of a given year (excluding headers)"""
        import subprocess
        import glob

        if year is None:
            year = self.TEST_YEAR

        # Get all CSV files for the specified year
        year_path = f'/Users/0x/code/trader/flatfiles/{year}'
        all_files = glob.glob(f'{year_path}/*/*.csv.gz')
        total_files = len(all_files)

        if total_files == 0:
            print(f"Warning: No CSV files found for year {year} in {year_path}")
            return 0

        if use_half_files:
            # Use only half the files (rounded up)
            half_count = (total_files + 1) // 2
            files_to_use = all_files[:half_count]
            expected_headers = half_count
        else:
            files_to_use = all_files
            expected_headers = total_files

        print(f"Counting rows from {len(files_to_use)} files for year {year} (expected headers: {expected_headers})")

        # Count lines from each file individually (excluding headers)
        total_data_rows = 0
        for file_path in files_to_use:
            result = subprocess.run(
                ['gunzip', '-c', file_path],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 0:  # Make sure file is not empty
                    # Subtract 1 header row per file
                    data_rows = len(lines) - 1
                    total_data_rows += data_rows
            else:
                print(f"Error reading {file_path}: {result.stderr}")

        return total_data_rows

    def test_year_full_processing_row_count_integrity(self):
        """Test processing of half the data for configured year and verify row count integrity"""
        from cleaner import process_year_files, validate_row_counts
        import glob

        year = self.TEST_YEAR

        # Get all files for the configured year and use only half of them
        all_files = sorted(glob.glob(f'flatfiles/{year}/*/*.csv.gz'))
        total_files = len(all_files)

        if total_files == 0:
            self.skipTest(f"No CSV files found for year {year}")

        half_count = (total_files + 1) // 2  # Round up
        files_to_process = all_files[:half_count]

        print(f"Processing {len(files_to_process)} out of {total_files} total files for year {year}")

        # Get expected input row count for the files we'll process
        expected_data_rows = self.get_expected_row_count(year=year, use_half_files=True)
        print(f"Expected data rows from CSV files (excluding headers): {expected_data_rows}")

        # Mock glob to return only the files we want to process
        with patch('cleaner.glob.glob', return_value=files_to_process):
            # Process the configured year with MinIO disabled for testing
            stats = process_year_files(year, minio=None)

        print("\nProcessing Statistics:")
        print(f"  Files processed: {stats['files_processed']}")
        print(f"  Total CSV rows read: {stats['total_csv_rows']}")
        print(f"  Tickers processed: {stats['tickers_processed']}")
        print(f"  Year results count: {stats['year_results_count']}")
        print(f"  Final DataFrame rows: {stats['final_df_rows']}")

        # Verify basic expectations
        self.assertEqual(stats['files_processed'], len(files_to_process), f"Should process {len(files_to_process)} CSV files for year {year}")

        # Verify row count integrity - should be close (allowing for data quality filtering)
        # Allow for up to 2% difference due to potential data quality issues
        tolerance = 0.02  # 2%
        min_expected = int(expected_data_rows * (1 - tolerance))
        max_expected = int(expected_data_rows * (1 + tolerance))

        self.assertGreaterEqual(
            stats['total_csv_rows'],
            min_expected,
            f"CSV rows read ({stats['total_csv_rows']}) should be at least {min_expected} (within 2% of expected {expected_data_rows})"
        )
        self.assertLessEqual(
            stats['total_csv_rows'],
            max_expected,
            f"CSV rows read ({stats['total_csv_rows']}) should be at most {max_expected} (within 2% of expected {expected_data_rows})"
        )

        # Verify that final output matches input (allowing for potential skips)
        self.assertLessEqual(
            stats['final_df_rows'],
            stats['total_csv_rows'],
            "Final DataFrame rows should not exceed input rows"
        )

        # Validate row count consistency
        validation_errors = validate_row_counts(stats)
        if validation_errors:
            print(f"Validation errors: {validation_errors}")

        # Allow for some flexibility - some tickers might be skipped due to data issues
        # but the final count should be reasonable (within 90% of input)
        min_expected_rows = int(stats['total_csv_rows'] * 0.9)
        self.assertGreaterEqual(
            stats['final_df_rows'],
            min_expected_rows,
            f"Final rows ({stats['final_df_rows']}) should be at least 90% of input rows ({min_expected_rows})"
        )

        print_success(f"Row count integrity verified for year {year}: {expected_data_rows} expected -> {stats['total_csv_rows']} read -> {stats['final_df_rows']} written")

    def test_year_data_quality_and_bad_row_handling(self):
        """Test data quality and handling of potentially bad rows for configured year"""
        import pandas as pd
        from cleaner import process_year_files, validate_row_counts
        import glob
        import os

        year = self.TEST_YEAR

        # Find a sample file for the configured year
        year_files = glob.glob(f'flatfiles/{year}/*/*.csv.gz')
        if not year_files:
            self.skipTest(f"No CSV files found for year {year}")

        # Use the first file as sample (typically the earliest date)
        sample_file = sorted(year_files)[0]
        sample_df = pd.read_csv(sample_file, compression='gzip')

        print(f"Sample file {sample_file} for year {year}:")
        print(f"  Shape: {sample_df.shape}")
        print(f"  Columns: {list(sample_df.columns)}")
        print(f"  Data types: {sample_df.dtypes.to_dict()}")

        # Check for missing values
        missing_values = sample_df.isnull().sum()
        print(f"  Missing values: {missing_values.to_dict()}")

        # Check for reasonable data ranges
        numeric_cols = ['volume', 'open', 'close', 'high', 'low', 'transactions']
        for col in numeric_cols:
            if col in sample_df.columns:
                min_val = sample_df[col].min()
                max_val = sample_df[col].max()
                print(f"  {col}: min={min_val}, max={max_val}")

        # Verify basic data integrity
        self.assertGreater(len(sample_df), 0, "Sample file should have data")
        self.assertIn('ticker', sample_df.columns, "Should have ticker column")
        self.assertIn('close', sample_df.columns, "Should have close price column")
        self.assertIn('window_start', sample_df.columns, "Should have timestamp column")

        # Test processing with error handling
        with patch('cleaner.write_to_minio'):
            stats = process_year_files(year, minio=None)

        # Verify that processing completed despite any potential data issues
        self.assertGreater(stats['files_processed'], 0, "Should process some files")
        self.assertGreater(stats['total_csv_rows'], 0, "Should read some rows")

        # Check that we handle bad rows appropriately
        # (This would be where rows get skipped due to data integrity issues)
        validation_errors = validate_row_counts(stats)
        print(f"Data quality validation: {len(validation_errors)} issues found")

        # Even with some data issues, we should have reasonable output
        if stats['total_csv_rows'] > 0:
            success_rate = stats['final_df_rows'] / stats['total_csv_rows']
            print(f"Success rate: {success_rate:.1%}")
            self.assertGreater(success_rate, 0.5, "Should successfully process at least 50% of rows")

        print_success(f"Data quality check passed for year {year} - {stats['final_df_rows']}/{stats['total_csv_rows']} rows processed successfully")

    def test_multi_year_kv_cache_persistence(self):
        """Test that KV cache persists across multiple years and improves indicator quality"""
        import glob
        from cleaner import process_year_files
        
        # Test with 2-3 years (2020-2022)
        test_years = [2020, 2021, 2022]
        
        # Skip test if not enough years have data
        available_years = []
        for year in test_years:
            files = glob.glob(f'flatfiles/{year}/*/*.csv.gz')
            if files:
                available_years.append(year)
        
        if len(available_years) < 2:
            self.skipTest(f"Need at least 2 years of data, found {len(available_years)}")
        
        # Use only first 2 available years for faster testing
        test_years = available_years[:2]
        print(f"\nTesting KV cache persistence across years: {test_years}")
        
        # Initialize caches
        lastclose = {}
        lasthi = {}
        lastlo = {}
        
        for idx, year in enumerate(test_years):
            print(f"\n--- Processing year {year} (year {idx + 1}/{len(test_years)}) ---")
            
            # Mock glob to use fewer files for speed (use first 30 files to ensure enough data for cache)
            all_files = sorted(glob.glob(f'flatfiles/{year}/*/*.csv.gz'))
            files_to_use = all_files[:30]  # Use 30 files to ensure tickers have >= 30 rows
            
            with patch('cleaner.glob.glob', return_value=files_to_use):
                stats = process_year_files(year, minio=None, 
                                          lastclose=lastclose, 
                                          lasthi=lasthi, 
                                          lastlo=lastlo)
            
            print(f"Stats for year {year}: {stats}")
            
            # After first year, cache should be populated
            if idx == 0:
                print(f"After year {year}:")
                print(f"  Cache contains {len(lastclose)} tickers")
                
                # Verify cache is populated
                self.assertGreater(len(lastclose), 0, 
                                 "Cache should contain data after first year")
                self.assertEqual(len(lastclose), len(lasthi), 
                               "All caches should have same tickers")
                self.assertEqual(len(lastclose), len(lastlo), 
                               "All caches should have same tickers")
                
                # Verify cache size is bounded (should be <= 60 rows per ticker)
                for ticker in list(lastclose.keys())[:5]:  # Check first 5 tickers
                    cache_size = len(lastclose[ticker])
                    print(f"  Ticker {ticker}: cache size = {cache_size}")
                    self.assertLessEqual(cache_size, 60, 
                                       f"Cache for {ticker} should be <= 60 rows")
                    self.assertGreaterEqual(cache_size, 30,
                                          f"Cache for {ticker} should be >= 30 rows (minimum threshold)")
            
            # After second year, verify cache keys still match
            if idx == 1:
                print(f"After year {year}:")
                print(f"  Cache still contains {len(lastclose)} tickers")
                
                # Cache should still be populated and potentially grown
                self.assertGreater(len(lastclose), 0,
                                 "Cache should still contain data after second year")
        
        print_success(f"Multi-year KV cache persistence test passed for years {test_years}")

    def test_indicators_with_vs_without_historical_context(self):
        """Test that indicators calculated with historical context have better quality"""
        
        # Use AAPL test data and split into two periods
        if not os.path.exists('test_data/AAPL_2020_full.csv'):
            self.skipTest("AAPL test data not available")
        
        df = pd.read_csv('test_data/AAPL_2020_full.csv')
        df = df.sort_values('window_start')
        
        # Need at least 80 rows to split meaningfully (40 for each period)
        if len(df) < 80:
            self.skipTest(f"Not enough AAPL data (need 80+, have {len(df)})")
        
        # Split data: first 60% = "year 1", remaining 40% = "year 2"
        split_point = int(len(df) * 0.6)
        df_year1 = df.iloc[:split_point].copy()
        df_year2 = df.iloc[split_point:].copy()
        
        # Add date column as expected by the function
        df_year1['date'] = pd.to_datetime(df_year1['window_start'], utc=True).dt.strftime("%Y-%m-%d")
        df_year2['date'] = pd.to_datetime(df_year2['window_start'], utc=True).dt.strftime("%Y-%m-%d")
        
        print(f"\nTesting indicator quality with/without historical context")
        print(f"  Year 1: {len(df_year1)} rows")
        print(f"  Year 2: {len(df_year2)} rows")
        
        # Scenario 1: Calculate year 2 WITHOUT historical context (fresh cache)
        result_without_cache = add_technical_indicators('AAPL', df_year2, {}, {}, {})
        
        # Scenario 2: Calculate year 1, then year 2 WITH historical context
        lastclose = {}
        lasthi = {}
        lastlo = {}
        
        # Process year 1 to populate cache
        result_year1 = add_technical_indicators('AAPL', df_year1, lastclose, lasthi, lastlo)
        self.assertIsNotNone(result_year1, "Year 1 processing should succeed")
        
        # Now cache is populated, process year 2
        result_with_cache = add_technical_indicators('AAPL', df_year2, lastclose, lasthi, lastlo)
        
        # Both should return valid results
        self.assertIsNotNone(result_without_cache, "Year 2 without cache should succeed")
        self.assertIsNotNone(result_with_cache, "Year 2 with cache should succeed")
        
        # Compare NaN counts in the indicators
        indicators = ['macd', 'bollub', 'bolllb', 'rsi30', 'sma30', 'sma60', 'cci30', 'dx30']
        
        print("\n  Comparing NaN counts (lower is better):")
        improvements = []
        for indicator in indicators:
            nan_without = result_without_cache[indicator].isna().sum()
            nan_with = result_with_cache[indicator].isna().sum()
            improvement = nan_without - nan_with
            improvements.append(improvement)
            
            print(f"    {indicator}: without_cache={nan_without} NaNs, with_cache={nan_with} NaNs, improvement={improvement}")
            
            # With cache, we should have equal or fewer NaNs
            self.assertLessEqual(nan_with, nan_without,
                               f"{indicator} should have <= NaNs with historical context")
        
        # At least some indicators should show improvement
        total_improvement = sum(improvements)
        print(f"\n  Total NaN reduction: {total_improvement} rows")
        self.assertGreater(total_improvement, 0,
                         "Historical context should reduce NaN count overall")
        
        # Verify that cached values are valid (not NaN/inf where they should have values)
        # Note: Values are EXPECTED to differ because cache provides more historical context,
        # which changes the calculation window for indicators like MACD, RSI, etc.
        # We just verify the data isn't corrupted (no unexpected NaN/inf)
        for indicator in indicators:
            # Check that non-NaN values are finite and reasonable
            non_nan_with_cache = result_with_cache[indicator].dropna()
            if len(non_nan_with_cache) > 0:
                self.assertTrue(np.all(np.isfinite(non_nan_with_cache)),
                              f"{indicator} has non-finite values with cache")
                
                # Verify values are in a reasonable range (not obviously corrupted)
                # For most indicators, values should be within a reasonable range
                if indicator in ['rsi30']:  # RSI should be 0-100
                    self.assertTrue(np.all(non_nan_with_cache >= 0) and np.all(non_nan_with_cache <= 100),
                                  f"{indicator} values out of expected range [0, 100]")
        
        print_success("Indicator quality comparison test passed - historical context reduces NaN count and produces valid values")


class TestTechnicalIndicators(unittest.TestCase):
    """Test technical indicator calculations using real 2020 data"""

    def setUp(self):
        """Set up test data"""
        self.test_tickers = ['AAPL', 'MSFT', 'TSLA', 'GOOGL']
        self.test_data = {}

        # Load test data for each ticker
        for ticker in self.test_tickers:
            file_path = f'test_data/{ticker}_2020_full.csv'
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df = df.sort_values('window_start')
                self.test_data[ticker] = df
                print(f"Loaded {len(df)} rows for {ticker}")
            else:
                print(f"Warning: Test data file not found for {ticker}")

    def test_macd_calculation(self):
        """Test MACD calculation against TA-Lib reference"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker]
            close = df['close']
            hi = df['high']
            lo = df['low']

            # Calculate using extracted function from cleaner.py
            result = calculate_indicators_for_series(close, hi, lo)

            # Calculate expected using TA-Lib directly
            macd_expected, _, _ = ta.MACD(close)

            # Verify the calculation matches
            np.testing.assert_array_almost_equal(
                result['macd'], macd_expected, decimal=6,
                err_msg=f"MACD calculation mismatch for {ticker}"
            )

            print_success(f"MACD test passed for {ticker}")

    def test_bollinger_bands_calculation(self):
        """Test Bollinger Bands calculation against TA-Lib reference"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker]
            close = df['close']
            hi = df['high']
            lo = df['low']

            # Calculate using extracted function from cleaner.py
            result = calculate_indicators_for_series(close, hi, lo)

            # Calculate expected using TA-Lib directly
            bollub_expected, _, bolllb_expected = ta.BBANDS(close)

            # Verify calculations match
            np.testing.assert_array_almost_equal(
                result['bollub'], bollub_expected, decimal=6,
                err_msg=f"Bollinger Upper Band mismatch for {ticker}"
            )
            np.testing.assert_array_almost_equal(
                result['bolllb'], bolllb_expected, decimal=6,
                err_msg=f"Bollinger Lower Band mismatch for {ticker}"
            )

            print_success(f"Bollinger Bands test passed for {ticker}")

    def test_rsi_calculation(self):
        """Test RSI calculation against TA-Lib reference"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker]
            close = df['close']
            hi = df['high']
            lo = df['low']

            # Calculate using extracted function from cleaner.py
            result = calculate_indicators_for_series(close, hi, lo)

            # Calculate expected using TA-Lib directly
            rsi_expected = ta.RSI(close, timeperiod=30)

            # Verify calculations match
            np.testing.assert_array_almost_equal(
                result['rsi30'], rsi_expected, decimal=6,
                err_msg=f"RSI(30) calculation mismatch for {ticker}"
            )

            print_success(f"RSI(30) test passed for {ticker}")

    def test_sma_calculation(self):
        """Test SMA calculations against TA-Lib reference"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker]
            close = df['close']
            hi = df['high']
            lo = df['low']

            # Calculate using extracted function from cleaner.py
            result = calculate_indicators_for_series(close, hi, lo)

            # Calculate expected using TA-Lib directly
            sma30_expected = ta.SMA(close, timeperiod=30)
            sma60_expected = ta.SMA(close, timeperiod=60)

            # Verify calculations match
            np.testing.assert_array_almost_equal(
                result['sma30'], sma30_expected, decimal=6,
                err_msg=f"SMA(30) calculation mismatch for {ticker}"
            )
            np.testing.assert_array_almost_equal(
                result['sma60'], sma60_expected, decimal=6,
                err_msg=f"SMA(60) calculation mismatch for {ticker}"
            )

            print_success(f"SMA tests passed for {ticker}")

    def test_cci_calculation(self):
        """Test CCI calculation against TA-Lib reference"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker]
            close = df['close']
            hi = df['high']
            lo = df['low']

            # Calculate using extracted function from cleaner.py
            result = calculate_indicators_for_series(close, hi, lo)

            # Calculate expected using TA-Lib directly
            cci_expected = ta.CCI(hi, lo, close, timeperiod=30)

            # Verify calculations match
            np.testing.assert_array_almost_equal(
                result['cci30'], cci_expected, decimal=6,
                err_msg=f"CCI(30) calculation mismatch for {ticker}"
            )

            print_success(f"CCI(30) test passed for {ticker}")

    def test_dx_calculation(self):
        """Test DX calculation against TA-Lib reference"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker]
            close = df['close']
            hi = df['high']
            lo = df['low']

            # Calculate using extracted function from cleaner.py
            result = calculate_indicators_for_series(close, hi, lo)

            # Calculate expected using TA-Lib directly
            dx_expected = ta.DX(hi, lo, close, timeperiod=30)

            # Verify calculations match
            np.testing.assert_array_almost_equal(
                result['dx30'], dx_expected, decimal=6,
                err_msg=f"DX(30) calculation mismatch for {ticker}"
            )

            print_success(f"DX(30) test passed for {ticker}")

    def test_add_technical_indicators_integration(self):
        """Test the full add_technical_indicators function with real data"""
        for ticker in self.test_tickers:
            if ticker not in self.test_data:
                continue

            df = self.test_data[ticker].copy()

            # Add date column as expected by the function (mimicking cleaner.py processing)
            df['date'] = pd.to_datetime(df['window_start'], utc=True).dt.strftime("%Y-%m-%d")

            # Test the full function
            result = add_technical_indicators(ticker, df, {}, {}, {})

            # Verify result is not None
            self.assertIsNotNone(result, f"add_technical_indicators returned None for {ticker}")

            # Verify expected columns are present
            expected_columns = ['macd', 'bollub', 'bolllb', 'rsi30', 'sma30', 'sma60', 'cci30', 'dx30']
            for col in expected_columns:
                self.assertIn(col, result.columns, f"Missing column {col} for {ticker}")

            # Verify we have reasonable data
            self.assertGreater(len(result), 0, f"No data returned for {ticker}")

            # Verify indicator values are reasonable (not all NaN)
            for col in expected_columns:
                non_nan_count = result[col].notna().sum()
                self.assertGreater(non_nan_count, 0, f"All {col} values are NaN for {ticker}")

            print_success(f"Integration test passed for {ticker} - {len(result)} rows with indicators")

    def test_indicator_calculation_accuracy_with_known_values(self):
        """Test indicator calculations with known input/output values"""
        # Use AAPL data for detailed accuracy testing
        if 'AAPL' not in self.test_data:
            self.skipTest("AAPL test data not available")

        df = self.test_data['AAPL']
        close = df['close'].values[:50]  # Use first 50 values for testing

        # Test SMA with known values
        sma_result = ta.SMA(close, timeperiod=10)
        self.assertIsNotNone(sma_result)

        # For SMA, the first 9 values should be NaN, then valid values
        self.assertTrue(np.isnan(sma_result[0]))
        self.assertTrue(np.isnan(sma_result[8]))
        self.assertFalse(np.isnan(sma_result[9]))

        # Test that SMA calculation is correct for a known case
        if len(close) >= 10:
            # Manual calculation of SMA for verification
            manual_sma = np.mean(close[0:10])
            self.assertAlmostEqual(sma_result[9], manual_sma, places=6,
                                 msg="SMA calculation doesn't match manual calculation")

        print_success("Known values accuracy test passed")


if __name__ == '__main__':
    unittest.main()
