#!/bin/bash
# Script to run all tests (unit + integration)

set -e

echo "=========================================="
echo "Running Go RabbitMQ Client Tests"
echo "=========================================="
echo ""

# Check if RabbitMQ is running
echo "Checking RabbitMQ connection..."
if ! timeout 5 bash -c 'cat < /dev/null > /dev/tcp/localhost/5672' 2>/dev/null; then
    echo ""
    echo "⚠️  WARNING: RabbitMQ is not running on localhost:5672"
    echo ""
    echo "Integration tests will be skipped."
    echo "To run integration tests, start RabbitMQ first:"
    echo "  ./start-rabbitmq.sh"
    echo ""
    echo "Running unit tests only..."
    echo ""
    go test -v ./rabbitmq/... -short
else
    echo "✅ RabbitMQ is running"
    echo ""
    echo "Running all tests (unit + integration)..."
    echo ""
    go test -v ./rabbitmq/...
fi

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
go test ./rabbitmq/... -v 2>&1 | grep -E "^(PASS|FAIL|ok|SKIP)" | tail -5
