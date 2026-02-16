#!/bin/bash
# Flink Stress Test Monitoring Script

echo "========================================="
echo "  Flink Stress Test Monitor"
echo "========================================="
echo ""

# Get job ID
JOB_ID=$(kubectl exec deployment/basic-session-cluster -- flink list 2>/dev/null | grep "RUNNING" | head -1 | awk '{print $4}' | tr -d ':')

if [ -z "$JOB_ID" ]; then
    echo "No running jobs found!"
    exit 1
fi

echo "Monitoring Job: $JOB_ID"
echo ""

# Monitoring loop
while true; do
    clear
    echo "========================================="
    echo "  Flink Performance Monitor"
    echo "  Job ID: $JOB_ID"
    echo "  Time: $(date '+%H:%M:%S')"
    echo "========================================="
    echo ""

    # Pod Status
    echo "📦 POD STATUS:"
    kubectl get pods -l app=basic-session-cluster --no-headers | \
        awk '{printf "  %-50s %s\n", $1, $3}'
    echo ""

    # Resource Usage
    echo "💻 RESOURCE USAGE:"
    kubectl top pods -l app=basic-session-cluster 2>/dev/null | \
        tail -n +2 | \
        awk '{printf "  %-50s CPU: %6s  Memory: %8s\n", $1, $2, $3}'
    echo ""

    # Flink Metrics
    echo "📊 FLINK METRICS:"

    # Try to get throughput
    THROUGHPUT=$(kubectl exec deployment/basic-session-cluster -- \
        curl -s "localhost:8081/jobs/$JOB_ID/metrics?get=numRecordsInPerSecond" 2>/dev/null | \
        grep -o '"value":"[0-9.]*"' | \
        grep -o '[0-9.]*' | head -1)

    if [ -n "$THROUGHPUT" ]; then
        printf "  Throughput: %.2f events/sec\n" "$THROUGHPUT"
    else
        echo "  Throughput: N/A"
    fi

    # Job status
    STATUS=$(kubectl exec deployment/basic-session-cluster -- \
        curl -s "localhost:8081/jobs/$JOB_ID" 2>/dev/null | \
        grep -o '"state":"[A-Z]*"' | \
        grep -o '[A-Z]*' | head -1)

    echo "  Job Status: $STATUS"

    # Checkpoint info
    echo ""
    echo "💾 CHECKPOINTS:"
    CHECKPOINT_COUNT=$(kubectl exec deployment/basic-session-cluster -- \
        curl -s "localhost:8081/jobs/$JOB_ID/checkpoints" 2>/dev/null | \
        grep -o '"latest":{"completed":{"id":[0-9]*' | \
        grep -o '[0-9]*$')

    if [ -n "$CHECKPOINT_COUNT" ]; then
        echo "  Latest Checkpoint ID: $CHECKPOINT_COUNT"
    else
        echo "  Checkpoints: Not configured"
    fi

    echo ""
    echo "========================================="
    echo "Press Ctrl+C to stop monitoring"
    echo "Refreshing in 3 seconds..."

    sleep 3
done
