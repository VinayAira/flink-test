# Flink Scaling Patterns and Best Practices

## Table of Contents
1. [Scaling Session Clusters](#scaling-session-clusters)
2. [Scaling Application Mode Jobs](#scaling-application-mode-jobs)
3. [Auto-scaling Strategies](#auto-scaling-strategies)
4. [Resource Optimization](#resource-optimization)

---

# 1. Scaling Session Clusters

## Horizontal Scaling (Add More TaskManagers)

### Before Scaling
```
Session Cluster
┌─────────────────────────────────────────┐
│ JobManager (2 GB)                       │
├─────────────────────────────────────────┤
│ TaskManager 1 (4 slots)                 │
│ ┌─────┬─────┬─────┬─────┐              │
│ │ J1  │ J1  │ J2  │ J2  │              │
│ └─────┴─────┴─────┴─────┘              │
├─────────────────────────────────────────┤
│ TaskManager 2 (4 slots)                 │
│ ┌─────┬─────┬─────┬─────┐              │
│ │ J1  │ J2  │ J3  │ J3  │              │
│ └─────┴─────┴─────┴─────┘              │
└─────────────────────────────────────────┘
Total Slots: 8
Jobs: J1 (4 slots), J2 (3 slots), J3 (2 slots)
Problem: Almost full! Can't add more jobs
```

### After Scaling (Adding 1 More TaskManager)
```
Session Cluster
┌─────────────────────────────────────────┐
│ JobManager (2 GB)                       │
├─────────────────────────────────────────┤
│ TaskManager 1 (4 slots)                 │
│ ┌─────┬─────┬─────┬─────┐              │
│ │ J1  │ J1  │ J2  │ J2  │              │
│ └─────┴─────┴─────┴─────┘              │
├─────────────────────────────────────────┤
│ TaskManager 2 (4 slots)                 │
│ ┌─────┬─────┬─────┬─────┐              │
│ │ J1  │ J2  │ J3  │ J3  │              │
│ └─────┴─────┴─────┴─────┘              │
├─────────────────────────────────────────┤
│ TaskManager 3 (4 slots) ⭐ NEW          │
│ ┌─────┬─────┬─────┬─────┐              │
│ │Empty│Empty│Empty│Empty│              │
│ └─────┴─────┴─────┴─────┘              │
└─────────────────────────────────────────┘
Total Slots: 12
Available for new jobs: 4 slots
```

### YAML for Scaling

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: session-cluster
spec:
  image: flink:1.20
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 2
  taskManager:
    replicas: 3  # Scale from 2 to 3
    resource:
      memory: "4096m"
      cpu: 2
```

### Scaling Command
```bash
# Scale TaskManagers
kubectl patch flinkdeployment session-cluster \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/taskManager/replicas", "value": 5}]'

# Verify scaling
kubectl get pods -l component=taskmanager
```

## Vertical Scaling (More Resources per TaskManager)

### Before Vertical Scaling
```
TaskManager (2 CPU, 4 GB, 4 slots)
┌──────────────────────────────────┐
│ Slot 1: Job Task (0.5 CPU, 1 GB)│
├──────────────────────────────────┤
│ Slot 2: Job Task (0.5 CPU, 1 GB)│
├──────────────────────────────────┤
│ Slot 3: Job Task (0.5 CPU, 1 GB)│
├──────────────────────────────────┤
│ Slot 4: Job Task (0.5 CPU, 1 GB)│
└──────────────────────────────────┘
Per-slot: 0.5 CPU, 1 GB
Problem: Jobs need more memory!
```

### After Vertical Scaling
```
TaskManager (4 CPU, 8 GB, 4 slots)
┌──────────────────────────────────┐
│ Slot 1: Job Task (1 CPU, 2 GB)  │
├──────────────────────────────────┤
│ Slot 2: Job Task (1 CPU, 2 GB)  │
├──────────────────────────────────┤
│ Slot 3: Job Task (1 CPU, 2 GB)  │
├──────────────────────────────────┤
│ Slot 4: Job Task (1 CPU, 2 GB)  │
└──────────────────────────────────┘
Per-slot: 1 CPU, 2 GB
Solution: Each task gets more resources!
```

### YAML for Vertical Scaling
```yaml
taskManager:
  replicas: 2
  resource:
    memory: "8192m"  # Doubled from 4096m
    cpu: 4           # Doubled from 2
```

---

# 2. Scaling Application Mode Jobs

## Scaling Individual Jobs

### Small Job → Medium Job
```
BEFORE: Order Processor (Light Load)
┌────────────────────────────────────┐
│ JobManager: 1 CPU, 2 GB            │
├────────────────────────────────────┤
│ TaskManager 1: 2 CPU, 4 GB         │
│ ┌────────┬────────┐                │
│ │Task 1  │Task 2  │                │
│ └────────┴────────┘                │
└────────────────────────────────────┘
Parallelism: 2
Throughput: 1,000 events/sec

AFTER: Order Processor (Heavy Load)
┌────────────────────────────────────┐
│ JobManager: 2 CPU, 4 GB            │
├────────────────────────────────────┤
│ TaskManager 1: 4 CPU, 8 GB         │
│ ┌────────┬────────┬────────┬─────┐│
│ │Task 1  │Task 2  │Task 3  │Task4││
│ └────────┴────────┴────────┴─────┘│
├────────────────────────────────────┤
│ TaskManager 2: 4 CPU, 8 GB         │
│ ┌────────┬────────┬────────┬─────┐│
│ │Task 5  │Task 6  │Task 7  │Task8││
│ └────────┴────────┴────────┴─────┘│
└────────────────────────────────────┘
Parallelism: 8
Throughput: 8,000 events/sec
```

### Scaling YAML

```yaml
# Before (Small)
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: order-processor
spec:
  image: flink:1.20
  flinkVersion: v1_20
  job:
    jarURI: local:///app/order-processor.jar
    parallelism: 2  # Small
  taskManager:
    replicas: 1
    resource:
      memory: "4096m"
      cpu: 2

---
# After (Large)
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: order-processor
spec:
  image: flink:1.20
  flinkVersion: v1_20
  job:
    jarURI: local:///app/order-processor.jar
    parallelism: 8  # Scaled up
  taskManager:
    replicas: 2     # More TaskManagers
    resource:
      memory: "8192m"  # More memory
      cpu: 4           # More CPU
```

## Operator-based Scaling

### Scaling with Savepoints (Zero Downtime)

```
Step 1: Take Savepoint
┌────────────────────────┐
│ Current Job (Running)  │
│ Parallelism: 4         │
└──────────┬─────────────┘
           │
           ▼
    ┌──────────────┐
    │  Savepoint   │
    │  s3://...    │
    └──────────────┘

Step 2: Stop Job
┌────────────────────────┐
│ Job Stopped            │
│ State saved            │
└────────────────────────┘

Step 3: Update Configuration
┌────────────────────────┐
│ New Configuration      │
│ Parallelism: 8         │
│ TaskManagers: 2        │
└────────────────────────┘

Step 4: Restore from Savepoint
┌────────────────────────┐
│ New Job (Running)      │
│ Parallelism: 8         │
│ State restored         │
└────────────────────────┘
```

### Commands for Savepoint-based Scaling

```bash
# 1. Trigger savepoint
kubectl patch flinkdeployment order-processor \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/job/state", "value": "suspended"}]'

# 2. Wait for savepoint to complete
kubectl get flinkdeployment order-processor -o yaml

# 3. Update parallelism and resources
kubectl patch flinkdeployment order-processor \
  --type='json' \
  -p='[
    {"op": "replace", "path": "/spec/job/parallelism", "value": 8},
    {"op": "replace", "path": "/spec/taskManager/replicas", "value": 2}
  ]'

# 4. Resume from savepoint
kubectl patch flinkdeployment order-processor \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/job/state", "value": "running"}]'
```

---

# 3. Auto-scaling Strategies

## Reactive Scaling (Built-in Flink Feature)

### How Reactive Scaling Works

```
Normal Operation:
┌─────────────────────────────────────────┐
│ JobManager detects:                     │
│ - Available TaskManagers                │
│ - Total slots available                 │
│ - Automatically adjusts parallelism     │
└─────────────────────────────────────────┘

┌────────────────┐
│ 2 TaskManagers │ → Parallelism: 8
│ 8 total slots  │
└────────────────┘

K8s HPA adds 1 TaskManager:
┌────────────────┐
│ 3 TaskManagers │ → Parallelism: 12 (auto!)
│ 12 total slots │
└────────────────┘
```

### Reactive Mode Configuration

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: reactive-job
spec:
  image: flink:1.20
  flinkVersion: v1_20
  flinkConfiguration:
    scheduler-mode: reactive  # Enable reactive mode
    taskmanager.numberOfTaskSlots: "4"
  job:
    jarURI: local:///app/processor.jar
    parallelism: -1  # Auto-parallelism
    upgradeMode: stateless
  taskManager:
    replicas: 2  # Starting replicas
    resource:
      memory: "4096m"
      cpu: 2
```

### Kubernetes HPA for Reactive Scaling

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: flink-taskmanager-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: reactive-job-taskmanager
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Pods
        value: 1
        periodSeconds: 60
```

### Reactive Scaling in Action

```
Timeline:

T=0min: Load = Low
┌──────────────────┐
│ 2 TaskManagers   │
│ Parallelism: 8   │
└──────────────────┘
CPU: 30%

T=5min: Load = High
┌──────────────────┐
│ 2 TaskManagers   │
│ Parallelism: 8   │
└──────────────────┘
CPU: 85% ⚠️ High!
HPA triggers scale-up

T=7min: Scaling in progress
┌──────────────────┐
│ 3 TaskManagers   │ ⬆️ +1 added
│ Parallelism: 12  │ 🔄 Auto-adjusted
└──────────────────┘
CPU: 55% ✅ Better

T=30min: Load = Low again
┌──────────────────┐
│ 3 TaskManagers   │
│ Parallelism: 12  │
└──────────────────┘
CPU: 20%
HPA waits (stabilization window)

T=35min: Scale down
┌──────────────────┐
│ 2 TaskManagers   │ ⬇️ -1 removed
│ Parallelism: 8   │ 🔄 Auto-adjusted
└──────────────────┘
CPU: 30% ✅ Optimal
```

## KEDA-based Auto-scaling

### KEDA Architecture

```
┌────────────────────────────────────────────────────┐
│                    KEDA                            │
│  (Kubernetes Event-driven Autoscaling)             │
└─────────────────┬──────────────────────────────────┘
                  │
                  │ Monitors
                  ▼
┌────────────────────────────────────────────────────┐
│         External Metrics Source                    │
│  - Kafka lag                                       │
│  - Queue depth                                     │
│  - Custom metrics                                  │
└─────────────────┬──────────────────────────────────┘
                  │
                  │ Triggers scaling
                  ▼
┌────────────────────────────────────────────────────┐
│         Flink TaskManagers                         │
│  Scales based on actual workload                  │
└────────────────────────────────────────────────────┘
```

### KEDA ScaledObject Example

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: flink-kafka-scaler
spec:
  scaleTargetRef:
    name: order-processor-taskmanager
  minReplicaCount: 2
  maxReplicaCount: 10
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka:9092
      consumerGroup: flink-consumer
      topic: orders
      lagThreshold: "1000"  # Scale when lag > 1000
      activationLagThreshold: "100"
```

---

# 4. Resource Optimization

## Slot Sizing Best Practices

### Under-provisioned Slots (Bad)

```
TaskManager: 4 CPU, 8 GB, 8 slots
┌──────────────────────────────────┐
│ Slot 1: 0.5 CPU, 1 GB            │ ⚠️ Too many slots!
│ Slot 2: 0.5 CPU, 1 GB            │    Tasks compete
│ Slot 3: 0.5 CPU, 1 GB            │    for resources
│ Slot 4: 0.5 CPU, 1 GB            │    Context switching
│ Slot 5: 0.5 CPU, 1 GB            │    overhead
│ Slot 6: 0.5 CPU, 1 GB            │
│ Slot 7: 0.5 CPU, 1 GB            │
│ Slot 8: 0.5 CPU, 1 GB            │
└──────────────────────────────────┘
Result: Poor performance, high GC
```

### Well-provisioned Slots (Good)

```
TaskManager: 4 CPU, 8 GB, 2 slots
┌──────────────────────────────────┐
│ Slot 1: 2 CPU, 4 GB              │ ✅ Good sizing
│ ┌──────────────────────────────┐ │    Enough resources
│ │ Sufficient memory for state  │ │    Low contention
│ │ Adequate CPU for processing  │ │    Better performance
│ └──────────────────────────────┘ │
├──────────────────────────────────┤
│ Slot 2: 2 CPU, 4 GB              │
│ ┌──────────────────────────────┐ │
│ │ Sufficient memory for state  │ │
│ │ Adequate CPU for processing  │ │
│ └──────────────────────────────┘ │
└──────────────────────────────────┘
Result: Good performance, stable
```

### Slot Sizing Formula

```
Recommended slots per TaskManager:
- Small TM (2 CPU, 4 GB): 1-2 slots
- Medium TM (4 CPU, 8 GB): 2-4 slots
- Large TM (8 CPU, 16 GB): 4-6 slots

Formula:
slots = max(1, CPU_cores / 2)

Example:
- 8 CPU → 4 slots
- 16 CPU → 8 slots
```

## Memory Configuration

### Memory Layout

```
TaskManager Memory (8 GB example)
┌────────────────────────────────────────┐
│  JVM Overhead (600 MB)                 │ 7.5%
├────────────────────────────────────────┤
│  Framework Heap (192 MB)               │ 2.4%
├────────────────────────────────────────┤
│  Task Heap (2.5 GB)                    │ 31.25%
│  - User code execution                 │
│  - Serialization buffers               │
├────────────────────────────────────────┤
│  Managed Memory (2.5 GB)               │ 31.25%
│  - RocksDB state backend               │
│  - Batch operators                     │
│  - Sorting/Hashing                     │
├────────────────────────────────────────┤
│  Network Memory (800 MB)               │ 10%
│  - Data exchange buffers               │
│  - Network buffers                     │
├────────────────────────────────────────┤
│  JVM Metaspace (256 MB)                │ 3.2%
└────────────────────────────────────────┘
Total: 8 GB (8192 MB)
```

### Memory Configuration YAML

```yaml
taskManager:
  resource:
    memory: "8192m"  # Total memory

flinkConfiguration:
  taskmanager.memory.process.size: "8192m"
  taskmanager.memory.framework.heap.size: "192m"
  taskmanager.memory.task.heap.size: "2560m"
  taskmanager.memory.managed.size: "2560m"
  taskmanager.memory.network.fraction: "0.1"
  taskmanager.memory.jvm-overhead.fraction: "0.1"
```

## Cost Optimization Matrix

```
┌─────────────────────────────────────────────────────────────┐
│                Deployment Cost Comparison                   │
│                (Running 10 Jobs)                            │
└─────────────────────────────────────────────────────────────┘

SESSION MODE:
┌──────────────────────────────────────┐
│ 1 Large Cluster                      │
│ JobManager: 2 CPU, 4 GB = $50/mo     │
│ TaskManagers: 8 × (4 CPU, 8 GB)      │
│             = 32 CPU, 64 GB          │
│             = $500/mo                │
│ ────────────────────────────────     │
│ Total: $550/mo                       │
│                                      │
│ Per Job: $55/mo                      │
└──────────────────────────────────────┘
Cheapest for many small jobs! ✅

APPLICATION MODE (Right-sized):
┌──────────────────────────────────────┐
│ 10 Individual Clusters               │
│ Each: JobManager (1 CPU, 2 GB)       │
│       TaskManager (2 CPU, 4 GB)      │
│ ────────────────────────────────     │
│ Per Cluster: $45/mo                  │
│ Total: $450/mo                       │
│                                      │
│ Per Job: $45/mo                      │
└──────────────────────────────────────┘
Good for right-sizing! ✅

APPLICATION MODE (Over-provisioned):
┌──────────────────────────────────────┐
│ 10 Individual Clusters               │
│ Each: JobManager (2 CPU, 4 GB)       │
│       TaskManagers: 2 × (4 CPU, 8GB) │
│ ────────────────────────────────────  │
│ Per Cluster: $150/mo                 │
│ Total: $1,500/mo                     │
│                                      │
│ Per Job: $150/mo                     │
└──────────────────────────────────────┘
Most expensive! ⚠️

Recommendation:
• Small jobs → Session Mode
• Large jobs → Application Mode (right-sized)
• Mixed → Hybrid approach
```

## Scaling Decision Tree

```
                      Start
                        │
                        ▼
              ┌──────────────────┐
              │ Current CPU      │
              │ Utilization?     │
              └────────┬─────────┘
                       │
      ┌────────────────┼────────────────┐
      │                │                │
   < 30%            30-70%            > 70%
      │                │                │
      ▼                ▼                ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│Scale Down│    │  Optimal │    │ Scale Up │
└──────────┘    └──────────┘    └──────────┘
      │                              │
      ▼                              ▼
┌──────────┐                  ┌──────────┐
│  Check:  │                  │  Check:  │
│• Lag     │                  │• Lag     │
│• Latency │                  │• Memory  │
│• Cost    │                  │• Errors  │
└──────────┘                  └──────────┘
      │                              │
      ▼                              ▼
Reduce by 1 TM              Add 1-2 TMs
Wait 5 min                  Wait 2 min
Monitor                     Monitor
```
