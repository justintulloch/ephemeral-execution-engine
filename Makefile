.PHONY: proto test test-unit test-steel lint typecheck clean install verify

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────

install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

# ──────────────────────────────────────────────
# Proto compilation
# ──────────────────────────────────────────────

proto:
	python -m grpc_tools.protoc \
		-Iproto \
		--python_out=. \
		--grpc_python_out=. \
		--mypy_out=. \
		proto/ember.proto

# ──────────────────────────────────────────────
# Testing
# ──────────────────────────────────────────────

test:
	python -m pytest tests/ -v

test-unit:
	python -m pytest tests/unit/ -v

test-steel:
	python -m pytest tests/steel_thread/ -v --timeout=60

# ──────────────────────────────────────────────
# Code quality
# ──────────────────────────────────────────────

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

typecheck:
	mypy core/ edge/ regional/ workers/ --ignore-missing-imports

# ──────────────────────────────────────────────
# Verification (run before merge)
# ──────────────────────────────────────────────

verify: lint typecheck test
	@echo "All checks passed."

# ──────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -name "*.pyc" -delete
	rm -f *_pb2.py *_pb2_grpc.py *_pb2.pyi
