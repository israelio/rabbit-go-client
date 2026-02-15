#!/bin/bash

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if RabbitMQ is running
check_rabbitmq() {
    echo -e "${BLUE}Checking RabbitMQ status...${NC}"

    if ! docker ps | grep -q rabbitmq; then
        echo -e "${YELLOW}RabbitMQ is not running. Starting it now...${NC}"
        docker run -d --name rabbitmq \
            -p 5672:5672 \
            -p 15672:15672 \
            rabbitmq:3-management

        echo -e "${YELLOW}Waiting for RabbitMQ to start (10 seconds)...${NC}"
        sleep 10
    fi

    echo -e "${GREEN}✓ RabbitMQ is running${NC}"
    echo -e "${BLUE}Management UI: http://localhost:15672 (guest/guest)${NC}\n"
}

# Show usage
usage() {
    echo -e "${BLUE}RabbitMQ Go Client - Example Runner${NC}\n"
    echo "Usage: $0 [example-name]"
    echo ""
    echo "Available examples:"
    echo "  1. basic          - Basic publish/consume (2 terminals)"
    echo "  2. confirms       - Publisher confirms"
    echo "  3. work-queue     - Work queue pattern (multiple workers)"
    echo "  4. rpc            - RPC client/server"
    echo "  5. topics         - Topic-based routing"
    echo "  6. recovery       - Automatic recovery"
    echo "  7. all            - Run quick demo of all examples"
    echo ""
    echo "Examples:"
    echo "  $0 basic"
    echo "  $0 confirms"
    echo "  $0 rpc"
    echo ""
}

# Basic example
run_basic() {
    echo -e "${GREEN}=== Basic Publish/Consume ===${NC}"
    echo -e "${YELLOW}This example requires 2 terminals:${NC}"
    echo ""
    echo -e "${BLUE}Terminal 1 (Consumer):${NC}"
    echo "  cd $(pwd)/basic_consume && go run main.go"
    echo ""
    echo -e "${BLUE}Terminal 2 (Publisher):${NC}"
    echo "  cd $(pwd)/basic_publish && go run main.go"
    echo ""
    echo -e "${YELLOW}Press Enter to continue...${NC}"
    read
}

# Publisher confirms
run_confirms() {
    echo -e "${GREEN}=== Publisher Confirms ===${NC}"
    cd publisher_confirms
    go run main.go
    cd ..
}

# Work queue
run_work_queue() {
    echo -e "${GREEN}=== Work Queue Pattern ===${NC}"
    echo -e "${YELLOW}Starting 2 workers in background...${NC}"

    cd work_queue
    go run worker.go &
    WORKER1_PID=$!
    go run worker.go &
    WORKER2_PID=$!

    sleep 2

    echo -e "${BLUE}Sending tasks...${NC}"
    go run new_task.go "Task 1 - Quick job"
    go run new_task.go "Task 2 - Medium job.."
    go run new_task.go "Task 3 - Long job....."

    echo -e "${YELLOW}Waiting for tasks to complete...${NC}"
    sleep 8

    kill $WORKER1_PID $WORKER2_PID 2>/dev/null
    cd ..

    echo -e "${GREEN}✓ Work queue demo complete${NC}"
}

# RPC
run_rpc() {
    echo -e "${GREEN}=== RPC Client/Server ===${NC}"
    echo -e "${YELLOW}Starting RPC server in background...${NC}"

    cd rpc_server
    go run main.go &
    RPC_PID=$!
    cd ..

    sleep 2

    echo -e "${BLUE}Making RPC calls...${NC}"
    cd rpc_client
    for i in {1..5}; do
        go run main.go $i
    done
    cd ..

    sleep 2
    kill $RPC_PID 2>/dev/null

    echo -e "${GREEN}✓ RPC demo complete${NC}"
}

# Topics
run_topics() {
    echo -e "${GREEN}=== Topic-based Routing ===${NC}"
    echo -e "${YELLOW}Starting topic consumer in background...${NC}"

    cd topics
    go run receive_logs_topic.go "#" &
    TOPIC_PID=$!

    sleep 2

    echo -e "${BLUE}Publishing logs...${NC}"
    go run emit_log_topic.go "auth.info" "User logged in"
    sleep 1
    go run emit_log_topic.go "auth.error" "Authentication failed"
    sleep 1
    go run emit_log_topic.go "payment.info" "Payment processed"
    sleep 1
    go run emit_log_topic.go "payment.error" "Payment declined"

    sleep 2
    kill $TOPIC_PID 2>/dev/null
    cd ..

    echo -e "${GREEN}✓ Topics demo complete${NC}"
}

# Recovery
run_recovery() {
    echo -e "${GREEN}=== Automatic Recovery ===${NC}"
    echo -e "${YELLOW}Starting recovery example...${NC}"
    echo -e "${BLUE}(Press Ctrl+C to stop)${NC}"
    echo ""
    echo -e "${YELLOW}To test recovery:${NC}"
    echo "  1. Let the example run for a few seconds"
    echo "  2. In another terminal, run: docker restart rabbitmq"
    echo "  3. Watch the example automatically reconnect"
    echo ""

    cd recovery
    go run main.go
    cd ..
}

# Run all examples
run_all() {
    echo -e "${GREEN}=== Running All Examples ===${NC}\n"

    echo -e "${BLUE}1/5: Publisher Confirms${NC}"
    run_confirms
    sleep 2

    echo -e "\n${BLUE}2/5: Work Queue${NC}"
    run_work_queue
    sleep 2

    echo -e "\n${BLUE}3/5: RPC${NC}"
    run_rpc
    sleep 2

    echo -e "\n${BLUE}4/5: Topics${NC}"
    run_topics
    sleep 2

    echo -e "\n${GREEN}✓ All automated examples complete!${NC}"
    echo -e "${YELLOW}Note: Basic and Recovery examples require manual interaction${NC}"
}

# Main script
main() {
    # Change to examples directory
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR"

    # Check RabbitMQ
    check_rabbitmq

    # Parse command
    case "${1:-help}" in
        basic)
            run_basic
            ;;
        confirms)
            run_confirms
            ;;
        work-queue|work)
            run_work_queue
            ;;
        rpc)
            run_rpc
            ;;
        topics)
            run_topics
            ;;
        recovery)
            run_recovery
            ;;
        all)
            run_all
            ;;
        help|--help|-h|*)
            usage
            ;;
    esac
}

main "$@"
