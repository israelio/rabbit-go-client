#!/bin/bash

# Test script for recovery example

set -e

echo "=== RabbitMQ Recovery Test ==="
echo ""

# Check if RabbitMQ is running
if ! docker ps | grep -q rabbitmq; then
    echo "Error: RabbitMQ is not running"
    echo "Start it with: docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management"
    exit 1
fi

echo "✓ RabbitMQ is running"
echo ""

# Start the recovery example in background
echo "Starting recovery example..."
go run main.go > /tmp/recovery.log 2>&1 &
RECOVERY_PID=$!

# Wait for it to start
sleep 3

echo "✓ Recovery example started (PID: $RECOVERY_PID)"
echo ""

# Show initial output
echo "Initial output:"
tail -10 /tmp/recovery.log
echo ""

# Wait a bit
echo "Waiting 5 seconds for messages..."
sleep 5
echo ""

# Restart RabbitMQ
echo "🔄 Restarting RabbitMQ..."
docker restart rabbitmq > /dev/null
echo "✓ RabbitMQ restarted"
echo ""

# Wait for recovery
echo "Waiting 10 seconds for recovery..."
sleep 10
echo ""

# Show output after recovery
echo "Output after recovery:"
tail -20 /tmp/recovery.log
echo ""

# Check if process is still running
if ps -p $RECOVERY_PID > /dev/null; then
    echo "✓ Recovery example is still running (successful recovery!)"

    # Stop the example
    kill $RECOVERY_PID 2>/dev/null
    wait $RECOVERY_PID 2>/dev/null

    echo "✓ Stopped recovery example"
else
    echo "✗ Recovery example exited (recovery failed)"
    exit 1
fi

echo ""
echo "=== Test Complete ==="
echo ""
echo "Full log available at: /tmp/recovery.log"
