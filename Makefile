.PHONY: test test-unit test-integration test-bench lint lint-fix coverage clean check ci install-tools install-hooks help

.DEFAULT_GOAL := help

## help: Display available commands
help:
	@echo "soy Development Commands"
	@echo "========================"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'

## test: Run all tests with race detector
test:
	@echo "Running all tests..."
	@go test -v -race -timeout=5m ./...

## test-unit: Run unit tests only (short mode)
test-unit:
	@echo "Running unit tests..."
	@go test -v -race -short -timeout=2m ./...

## test-integration: Run integration tests
test-integration:
	@echo "Running integration tests..."
	@go test -v -race -timeout=10m ./testing/integration/...

## test-bench: Run benchmarks
test-bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=100ms -timeout=5m ./testing/benchmarks/...

## lint: Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run --config=.golangci.yml --timeout=5m

## lint-fix: Run linters with auto-fix
lint-fix:
	@echo "Running linters with auto-fix..."
	@golangci-lint run --config=.golangci.yml --fix

## coverage: Generate coverage report
coverage:
	@echo "Generating coverage report..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report generated: coverage.html"

## clean: Remove generated files
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete

## check: Quick validation (lint + unit tests)
check: lint test-unit
	@echo "All checks passed!"

## ci: Full CI simulation
ci: clean lint test coverage
	@echo "Full CI simulation complete!"

## install-tools: Install required development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

## install-hooks: Install git pre-commit hook
install-hooks:
	@echo "Installing git hooks..."
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make check' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed!"
