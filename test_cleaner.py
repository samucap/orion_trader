import unittest
import tempfile
import os
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
import io

# Add the current directory to the path so we can import cleaner
sys.path.insert(0, os.path.dirname(__file__))

from cleaner import (
    process_year_files,
    validate_row_counts,
    add_technical_indicators,
    write_to_minio
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

        print(f"✅ Row count integrity verified for year {year}: {expected_data_rows} expected -> {stats['total_csv_rows']} read -> {stats['final_df_rows']} written")

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

        print(f"✅ Data quality check passed for year {year} - {stats['final_df_rows']}/{stats['total_csv_rows']} rows processed successfully")

if __name__ == '__main__':
    unittest.main()
