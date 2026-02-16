#!/bin/bash
# Real-time monitoring of Flink pods

echo "=== Monitoring Flink Pod Resources ==="
echo "Press Ctrl+C to stop"
echo ""

watch -n 2 'kubectl top pods --selector=app=basic-session-cluster && echo "" && kubectl get pods --selector=app=basic-session-cluster -o custom-columns=NAME:.metadata.name,CPU_REQUEST:.spec.containers[0].resources.requests.cpu,CPU_LIMIT:.spec.containers[0].resources.limits.cpu,MEM_REQUEST:.spec.containers[0].resources.requests.memory,MEM_LIMIT:.spec.containers[0].resources.limits.memory,STATUS:.status.phase'
