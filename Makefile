# Set default shell
SHELL := /bin/bash

# --- Environment Setup ---
# Check if .env file exists, if not, create it from the example
.env:
	@echo "Creating .env file from .env.example..."
	@cp .env.example .env

# --- Docker Compose Commands ---

# Build and start all services in detached mode
dev: .env
	@echo "Starting development environment..."
	docker-compose up --build -d
	#docker-compose up --build -d

# Stop and remove all services, networks, and volumes
down:
	@echo "Stopping development environment..."
	docker-compose down

# View logs from all services
logs:
	@echo "Tailing logs..."
	docker-compose logs -f

# --- Go Application Commands ---

# Build the Go binary for the local OS
build:
	@echo "Building Go application..."
	go build -o bin/oriontrader_pipeline .

# Run application tests
test:
	@echo "Running tests..."
	go test -v ./...

# Tidy Go modules
tidy:
	@echo "Tidying go modules..."
	go mod tidy

# --- Python Test Commands ---

# Run Python tests (default: all tests)
pytest:
	@echo "Running Python tests..."
	@if [ -z "$(TEST)" ]; then \
		./trader/bin/python3 test_cleaner.py; \
	else \
		./trader/bin/python3 -c "import unittest; from test_cleaner import TestCleanerRealData; suite = unittest.TestSuite(); suite.addTest(TestCleanerRealData('$(TEST)')); runner = unittest.TextTestRunner(verbosity=2); result = runner.run(suite)"; \
	fi

# Run Python tests with specific year (default: 2020)
pytest-year:
	@echo "Running Python tests for year $(YEAR)..."
	@if [ -z "$(YEAR)" ]; then \
		echo "Using default year 2020"; \
		./trader/bin/python3 test_cleaner.py; \
	else \
		echo "Testing year $(YEAR)"; \
		TEST_YEAR=$(YEAR) ./trader/bin/python3 test_cleaner.py; \
	fi

# Run Python tests with coverage (simplified without pytest-cov)
pytest-cov:
	@echo "Running Python tests with verbose output..."
	@if [ -z "$(TEST)" ]; then \
		./trader/bin/python3 test_cleaner.py; \
	else \
		./trader/bin/python3 -c "import unittest; from test_cleaner import TestCleanerRealData; suite = unittest.TestSuite(); suite.addTest(TestCleanerRealData('$(TEST)')); runner = unittest.TextTestRunner(verbosity=2); result = runner.run(suite)"; \
	fi

# Run specific Python test method (usage: make pytest-method TEST=test_2020_full_processing_row_count_integrity)
pytest-method:
	@echo "Running Python test method: $(TEST)"
	@if [ -z "$(TEST)" ]; then \
		echo "Error: Please specify TEST variable. Usage: make pytest-method TEST=test_method_name"; \
		exit 1; \
	fi
	./trader/bin/python3 -c "import unittest; from test_cleaner import TestCleanerRealData; suite = unittest.TestSuite(); suite.addTest(TestCleanerRealData('$(TEST)')); runner = unittest.TextTestRunner(verbosity=2); result = runner.run(suite)"

# Run Python tests using unittest directly
unittest:
	@echo "Running Python tests with unittest..."
	@if [ -z "$(TEST)" ]; then \
		./trader/bin/python3 test_cleaner.py; \
	else \
		./trader/bin/python3 -c "import unittest; from test_cleaner import TestCleanerRealData; suite = unittest.TestSuite(); suite.addTest(TestCleanerRealData('$(TEST)')); runner = unittest.TextTestRunner(verbosity=2); result = runner.run(suite)"; \
	fi

# --- Help ---
# Display this help message
help:
	@echo "Available commands:"
	@echo "  dev      - Start all services (Go app, MinIO) using docker-compose."
	@echo "  down     - Stop and remove all services."
	@echo "  logs     - Tail logs from running services."
	@echo "  build    - Build the Go application binary locally."
	@echo "  test     - Run Go unit tests."
	@echo "  tidy     - Tidy go.mod and go.sum files."
	@echo ""
	@echo "Python test commands:"
	@echo "  pytest          - Run all Python tests."
	@echo "  pytest TEST=method_name - Run specific Python test method."
	@echo "  pytest-cov      - Run Python tests with coverage report."
	@echo "  pytest-method TEST=method_name - Run specific test method."
	@echo "  unittest        - Run Python tests using unittest directly."
	@echo "  unittest TEST=method_name - Run specific test with unittest."

# Phony targets are not actual files
.PHONY: dev down logs build test tidy help pytest pytest-cov pytest-method pytest-year unittest
