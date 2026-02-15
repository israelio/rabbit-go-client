#!/bin/bash
# Script to start RabbitMQ for integration testing

set -e

CONTAINER_NAME="rabbitmq-test"
RABBITMQ_IMAGE="rabbitmq:3-management"

echo "Starting RabbitMQ container..."

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container ${CONTAINER_NAME} already exists."

    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Container is already running."
    else
        echo "Starting existing container..."
        docker start ${CONTAINER_NAME}
    fi
else
    echo "Creating new RabbitMQ container..."
    docker run -d \
        --name ${CONTAINER_NAME} \
        -p 5672:5672 \
        -p 15672:15672 \
        -e RABBITMQ_DEFAULT_USER=guest \
        -e RABBITMQ_DEFAULT_PASS=guest \
        ${RABBITMQ_IMAGE}
fi

echo ""
echo "Waiting for RabbitMQ to be ready..."
sleep 10

# Wait for RabbitMQ to be ready
MAX_RETRIES=30
RETRY_COUNT=0
while ! docker exec ${CONTAINER_NAME} rabbitmq-diagnostics -q ping 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: RabbitMQ failed to start after ${MAX_RETRIES} attempts"
        exit 1
    fi
    echo "Waiting for RabbitMQ to be ready... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

echo ""
echo "✅ RabbitMQ is ready!"
echo ""
echo "Connection details:"
echo "  AMQP URL:        amqp://guest:guest@localhost:5672/"
echo "  Management UI:   http://localhost:15672"
echo "  Username:        guest"
echo "  Password:        guest"
echo ""
echo "To stop RabbitMQ:  docker stop ${CONTAINER_NAME}"
echo "To remove:         docker rm -f ${CONTAINER_NAME}"
echo "To view logs:      docker logs -f ${CONTAINER_NAME}"
echo ""
