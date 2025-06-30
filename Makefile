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

# Phony targets are not actual files
.PHONY: dev down logs build test tidy help
