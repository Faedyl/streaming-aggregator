.PHONY: help build up down test test-dedup test-api test-perf test-persistence clean

help:
	@echo "UAS Pub-Sub Log Aggregator - Available Commands"
	@echo ""
	@echo "Docker:"
	@echo "  make build       - Build all Docker images"
	@echo "  make up          - Start all services (docker compose up)"
	@echo "  make down        - Stop all services"
	@echo "  make logs        - View logs"
	@echo ""
	@echo "Testing:"
	@echo "  make test        - Run all tests"
	@echo "  make test-dedup  - Run dedup tests"
	@echo "  make test-api    - Run API tests"
	@echo "  make test-perf   - Run performance tests"
	@echo "  make test-persist - Run persistence tests"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean       - Clean temporary files"

build:
	@echo "Building Docker images..."
	docker compose build

up:
	@echo "Starting services..."
	docker compose up --build -d
	@echo "Aggregator at: http://localhost:8080"
	@echo "Waiting for healthcheck..."
	@sleep 10
	@curl -s http://localhost:8080/health | python -m json.tool

down:
	@echo "Stopping services..."
	docker compose down

logs:
	docker compose logs -f

test:
	@echo "Running all tests..."
	cd aggregator && DATABASE_URL=postgres://struser:strpass@localhost:5432/strdb python -m pytest tests/ -v --tb=short

test-dedup:
	cd aggregator && DATABASE_URL=postgres://struser:strpass@localhost:5432/strdb python -m pytest tests/test_dedup.py -v --tb=short

test-api:
	cd aggregator && DATABASE_URL=postgres://struser:strpass@localhost:5432/strdb python -m pytest tests/test_api.py -v --tb=short

test-perf:
	cd aggregator && DATABASE_URL=postgres://struser:strpass@localhost:5432/strdb python -m pytest tests/test_performance.py -v --tb=short -s

test-persist:
	cd aggregator && DATABASE_URL=postgres://struser:strpass@localhost:5432/strdb python -m pytest tests/test_persistence.py -v --tb=short

clean:
	@echo "Cleaning..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov *.egg-info
