# Apache Flink Deployment Types - Complete Guide

## Table of Contents
1. [Session Mode](#session-mode)
2. [Application Mode](#application-mode)
3. [Per-Job Mode (Deprecated)](#per-job-mode)
4. [Comparison Matrix](#comparison-matrix)
5. [Architecture Diagrams](#architecture-diagrams)
6. [Real-world Examples](#real-world-examples)

---

# 1. Session Mode

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     SESSION CLUSTER                             │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              Flink JobManager (Shared)                    │ │
│  │  - Manages all jobs                                       │ │
│  │  - Shared resource scheduling                             │ │
│  │  - Single point of coordination                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                              │                                  │
│         ┌────────────────────┼────────────────────┐           │
│         │                    │                    │            │
│  ┌──────▼──────┐      ┌──────▼──────┐     ┌──────▼──────┐   │
│  │TaskManager 1│      │TaskManager 2│     │TaskManager 3│    │
│  │             │      │             │     │             │     │
│  │ ┌────────┐ │      │ ┌────────┐ │     │ ┌────────┐ │     │
│  │ │Job A   │ │      │ │Job A   │ │     │ │Job B   │ │     │
│  │ │Task 1  │ │      │ │Task 2  │ │     │ │Task 1  │ │     │
│  │ └────────┘ │      │ └────────┘ │     │ └────────┘ │     │
│  │ ┌────────┐ │      │ ┌────────┐ │     │ ┌────────┐ │     │
│  │ │Job B   │ │      │ │Job C   │ │     │ │Job C   │ │     │
│  │ │Task 2  │ │      │ │Task 1  │ │     │ │Task 2  │ │     │
│  │ └────────┘ │      │ └────────┘ │     │ └────────┘ │     │
│  └─────────────┘      └─────────────┘     └─────────────┘     │
│                                                                 │
│  Multiple jobs sharing the same cluster resources              │
└─────────────────────────────────────────────────────────────────┘
```

## Lifecycle Diagram

```
Time ──────────────────────────────────────────────────────▶

Step 1: Start Cluster
┌─────────────────────┐
│ Start Flink Cluster │
│ JobManager + TMs    │
└─────────────────────┘
         │
         ▼
Step 2: Submit Jobs (Anytime)
┌─────────────────────┐     ┌─────────────────────┐
│  Submit Job A       │     │  Submit Job B       │
│  (t=0)              │     │  (t=10s)            │
└─────────────────────┘     └─────────────────────┘
         │                           │
         ▼                           ▼
Step 3: Jobs Running
┌────────────────────────────────────────────┐
│  Cluster Running                           │
│  - Job A executing                         │
│  - Job B executing                         │
│  - Resources shared between jobs           │
└────────────────────────────────────────────┘
         │
         ▼
Step 4: Job Completion
┌─────────────────────┐     ┌─────────────────────┐
│  Job A completes    │     │  Job B completes    │
│  (t=60s)            │     │  (t=120s)           │
│  Resources freed    │     │  Resources freed    │
└─────────────────────┘     └─────────────────────┘
         │
         ▼
Step 5: Cluster Still Alive
┌─────────────────────┐
│ Cluster remains     │
│ Ready for new jobs  │
└─────────────────────┘
```

## YAML Example

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
    state.backend: filesystem
    # Shared configuration for all jobs
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 2
  taskManager:
    replicas: 3
    resource:
      memory: "4096m"
      cpu: 2
  # NO job specification - just the cluster
```

## Resource Sharing Visualization

```
TaskManager 1 (4 slots)
┌──────────────────────────────────────┐
│ Slot 1: Job A Task 1                 │
├──────────────────────────────────────┤
│ Slot 2: Job A Task 2                 │
├──────────────────────────────────────┤
│ Slot 3: Job B Task 1                 │
├──────────────────────────────────────┤
│ Slot 4: Job C Task 1                 │
└──────────────────────────────────────┘

Multiple jobs competing for the same slots!
```

## Use Cases

✅ **Good for:**
- Development and testing
- Interactive queries
- Multiple short-lived jobs
- Exploratory data analysis
- Quick prototyping

❌ **Not good for:**
- Production critical workloads
- Jobs requiring isolation
- Long-running streaming pipelines
- When resource guarantees are needed

---

# 2. Application Mode

## Architecture Diagram

```
Application Mode Deployment 1          Application Mode Deployment 2
┌────────────────────────────┐        ┌────────────────────────────┐
│   Job A Cluster            │        │   Job B Cluster            │
│                            │        │                            │
│  ┌──────────────────────┐ │        │  ┌──────────────────────┐ │
│  │  JobManager          │ │        │  │  JobManager          │ │
│  │  (Dedicated for A)   │ │        │  │  (Dedicated for B)   │ │
│  └──────────────────────┘ │        │  └──────────────────────┘ │
│           │                │        │           │                │
│     ┌─────┴─────┐         │        │     ┌─────┴─────┐         │
│     │           │          │        │     │           │          │
│  ┌──▼──┐     ┌──▼──┐      │        │  ┌──▼──┐     ┌──▼──┐      │
│  │ TM1 │     │ TM2 │      │        │  │ TM1 │     │ TM2 │      │
│  │     │     │     │       │        │  │     │     │     │       │
│  │Job A│     │Job A│      │        │  │Job B│     │Job B│      │
│  │Only │     │Only │      │        │  │Only │     │Only │      │
│  └─────┘     └─────┘      │        │  └─────┘     └─────┘      │
│                            │        │                            │
│  Complete Isolation        │        │  Complete Isolation        │
└────────────────────────────┘        └────────────────────────────┘

Each job has its own dedicated cluster - NO resource sharing
```

## Lifecycle Diagram

```
Time ──────────────────────────────────────────────────────▶

Job A Lifecycle:
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│Deploy Job A  │───▶│Cluster Starts│───▶│Job Runs      │
│YAML          │    │JobManager+TMs│    │              │
└──────────────┘    └──────────────┘    └──────┬───────┘
                                               │
                                               ▼
                                        ┌──────────────┐
                                        │Job Completes │
                                        │Cluster Dies  │
                                        └──────────────┘

Job B Lifecycle (Separate):
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│Deploy Job B  │───▶│Cluster Starts│───▶│Job Runs      │
│YAML          │    │JobManager+TMs│    │              │
└──────────────┘    └──────────────┘    └──────┬───────┘
                                               │
                                               ▼
                                        ┌──────────────┐
                                        │Job Completes │
                                        │Cluster Dies  │
                                        └──────────────┘

Each job has independent lifecycle!
```

## YAML Example

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: order-processor-app
spec:
  image: flink:1.20
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.backend: rocksdb
    state.checkpoints.dir: s3://my-bucket/checkpoints
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 2
  taskManager:
    replicas: 5  # Sized specifically for this job
    resource:
      memory: "8192m"  # Lots of memory for this job
      cpu: 4
  job:
    jarURI: local:///app/order-processor.jar
    entryClass: com.company.OrderProcessor
    args: ["--bootstrap.servers", "kafka:9092"]
    parallelism: 10
    upgradeMode: stateless
    state: running
```

## Resource Allocation Visualization

```
Job A Deployment (Order Processing - High Priority)
┌────────────────────────────────────────────────────────┐
│ JobManager: 2 CPU, 2 GB RAM                            │
├────────────────────────────────────────────────────────┤
│ TaskManager 1: 4 CPU, 8 GB RAM                         │
│ ┌──────────┬──────────┬──────────┬──────────┐         │
│ │  Slot 1  │  Slot 2  │  Slot 3  │  Slot 4  │         │
│ │  Job A   │  Job A   │  Job A   │  Job A   │         │
│ └──────────┴──────────┴──────────┴──────────┘         │
├────────────────────────────────────────────────────────┤
│ TaskManager 2: 4 CPU, 8 GB RAM                         │
│ ┌──────────┬──────────┬──────────┬──────────┐         │
│ │  Slot 1  │  Slot 2  │  Slot 3  │  Slot 4  │         │
│ │  Job A   │  Job A   │  Job A   │  Job A   │         │
│ └──────────┴──────────┴──────────┴──────────┘         │
└────────────────────────────────────────────────────────┘
Total: 8 CPU, 18 GB RAM dedicated to Job A

Job B Deployment (Analytics - Lower Priority)
┌────────────────────────────────────────────────────────┐
│ JobManager: 1 CPU, 1 GB RAM                            │
├────────────────────────────────────────────────────────┤
│ TaskManager 1: 2 CPU, 4 GB RAM                         │
│ ┌──────────┬──────────┐                                │
│ │  Slot 1  │  Slot 2  │                                │
│ │  Job B   │  Job B   │                                │
│ └──────────┴──────────┘                                │
└────────────────────────────────────────────────────────┘
Total: 3 CPU, 5 GB RAM dedicated to Job B

Each job gets exactly what it needs!
```

## Use Cases

✅ **Good for:**
- Production workloads
- Long-running streaming jobs
- Critical business applications
- Jobs requiring isolation
- Fine-grained resource control
- Multi-tenant environments

❌ **Not good for:**
- Quick testing (slow startup)
- Many short-lived jobs
- Limited cluster resources
- Development environments

---

# 3. Per-Job Mode (Deprecated)

## Architecture Diagram

```
⚠️  DEPRECATED - Use Application Mode instead

┌────────────────────────────────────────────────────────┐
│              Per-Job Mode (Legacy)                     │
│                                                        │
│  Similar to Application Mode but:                     │
│  - Job submission happens differently                 │
│  - Client must stay connected during submission       │
│  - Less efficient resource usage                      │
│                                                        │
│  ┌──────────────────────────────────────────────┐    │
│  │         JobManager (for single job)          │    │
│  └──────────────────────────────────────────────┘    │
│                      │                                │
│         ┌────────────┴────────────┐                  │
│         │                         │                   │
│  ┌──────▼──────┐          ┌──────▼──────┐           │
│  │TaskManager 1│          │TaskManager 2│            │
│  │  (Job only) │          │  (Job only) │            │
│  └─────────────┘          └─────────────┘            │
└────────────────────────────────────────────────────────┘

Replaced by Application Mode in newer Flink versions
```

---

# 4. Comparison Matrix

## Feature Comparison Table

```
┌──────────────────────┬─────────────────┬─────────────────┬─────────────────┐
│      Feature         │  Session Mode   │ Application Mode│  Per-Job Mode   │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Cluster Lifecycle    │ Long-lived      │ Per-job         │ Per-job         │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Resource Isolation   │ ❌ No (Shared)  │ ✅ Yes (Isolated)│ ✅ Yes          │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Job Startup Time     │ ⚡ Fast         │ 🐌 Slow         │ 🐌 Slow         │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Resource Efficiency  │ 👍 Good for     │ 👍 Good for     │ 👎 Poor         │
│                      │    many small   │    few large    │                 │
│                      │    jobs         │    jobs         │                 │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Production Ready     │ ❌ No           │ ✅ Yes          │ ⚠️  Deprecated  │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Job Submission       │ Interactive     │ Declarative     │ CLI-based       │
│                      │ (CLI/REST/UI)   │ (YAML)          │                 │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Fault Isolation      │ ❌ One bad job  │ ✅ Jobs isolated│ ✅ Jobs isolated│
│                      │    affects all  │                 │                 │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Resource Guarantees  │ ❌ No guarantees│ ✅ Guaranteed   │ ✅ Guaranteed   │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Scaling              │ Manual/Complex  │ Per-job scaling │ Per-job scaling │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Multi-tenancy        │ ❌ Difficult    │ ✅ Easy         │ ⚠️  Possible    │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ K8s Native           │ ✅ Yes          │ ✅ Yes (Best)   │ ⚠️  Limited     │
├──────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Best For             │ Dev/Testing     │ Production      │ Legacy          │
└──────────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

## Resource Usage Comparison

```
Scenario: Running 3 Jobs

SESSION MODE:
┌─────────────────────────────────────────┐
│         One Shared Cluster              │
│  JobManager: 2 GB                       │
│  TaskManager: 8 GB × 3 = 24 GB          │
│  ────────────────────────────────────   │
│  Total: 26 GB for all 3 jobs            │
└─────────────────────────────────────────┘
Resource Efficiency: ⭐⭐⭐⭐⭐

APPLICATION MODE:
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│   Job A       │ │   Job B       │ │   Job C       │
│ JM: 2 GB      │ │ JM: 2 GB      │ │ JM: 2 GB      │
│ TM: 8 GB × 2  │ │ TM: 8 GB × 2  │ │ TM: 8 GB × 2  │
│ Total: 18 GB  │ │ Total: 18 GB  │ │ Total: 18 GB  │
└───────────────┘ └───────────────┘ └───────────────┘
Total: 54 GB for all 3 jobs
Resource Efficiency: ⭐⭐⭐
Better isolation, higher cost
```

## Decision Flow Chart

```
                    Start: Need to run Flink job
                                │
                                ▼
                    ┌───────────────────────┐
                    │ Is this for           │
                    │ Production?           │
                    └───────┬───────────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
           Yes│                           │No
              ▼                           ▼
    ┌─────────────────┐         ┌─────────────────┐
    │ Need job        │         │ Multiple small  │
    │ isolation?      │         │ jobs?           │
    └────┬────────────┘         └────┬────────────┘
         │                           │
    Yes  │                      Yes  │         No
         ▼                           ▼          │
    ┌─────────────┐           ┌──────────┐     │
    │APPLICATION  │           │ SESSION  │     │
    │   MODE      │◀──────────│   MODE   │◀────┘
    │             │           │          │
    │✅ Use This  │           │✅ Use This│
    └─────────────┘           └──────────┘
```

---

# 5. Architecture Diagrams

## Complete Kubernetes Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                                  │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │            Flink Kubernetes Operator (Control Plane)             │ │
│  │  - Watches FlinkDeployment CRDs                                  │ │
│  │  - Creates/manages pods, services, configmaps                    │ │
│  │  - Handles failures and scaling                                  │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                              │                                         │
│                              │ Manages                                 │
│         ┌────────────────────┼────────────────────┐                   │
│         │                    │                    │                    │
│         ▼                    ▼                    ▼                    │
│  ┌─────────────┐      ┌─────────────┐     ┌─────────────┐           │
│  │  Session    │      │ Application │     │ Application │            │
│  │  Cluster    │      │   Job A     │     │   Job B     │            │
│  │             │      │             │     │             │             │
│  │ ┌─────────┐ │      │ ┌─────────┐ │     │ ┌─────────┐ │           │
│  │ │JobMgr   │ │      │ │JobMgr   │ │     │ │JobMgr   │ │           │
│  │ │Pod      │ │      │ │Pod      │ │     │ │Pod      │ │           │
│  │ └─────────┘ │      │ └─────────┘ │     │ └─────────┘ │           │
│  │      │      │      │      │      │     │      │      │            │
│  │   ┌──┴──┐   │      │   ┌──┴──┐   │     │   ┌──┴──┐   │           │
│  │   │     │   │      │   │     │   │     │   │     │   │            │
│  │ ┌─▼─┐ ┌─▼─┐ │      │ ┌─▼─┐ ┌─▼─┐ │     │ ┌─▼─┐ ┌─▼─┐ │           │
│  │ │TM │ │TM │ │      │ │TM │ │TM │ │     │ │TM │ │TM │ │           │
│  │ │Pod│ │Pod│ │      │ │Pod│ │Pod│ │     │ │Pod│ │Pod│ │           │
│  │ └───┘ └───┘ │      │ └───┘ └───┘ │     │ └───┘ └───┘ │           │
│  │             │      │             │     │             │             │
│  │ Service:    │      │ Service:    │     │ Service:    │             │
│  │ :8081       │      │ :8081       │     │ :8081       │             │
│  └─────────────┘      └─────────────┘     └─────────────┘             │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │                     Persistent Storage                           │ │
│  │  - Checkpoints (S3, HDFS, etc.)                                  │ │
│  │  - Savepoints                                                    │ │
│  │  - State backends                                                │ │
│  └──────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

## Network Communication

```
┌─────────────────────────────────────────────────────────────────┐
│                  Flink Cluster Network                          │
│                                                                 │
│  External Access                                                │
│       │                                                         │
│       │ Port 8081 (REST API / Web UI)                          │
│       ▼                                                         │
│  ┌─────────────────────────────────┐                           │
│  │      JobManager Service         │                           │
│  │  - ClusterIP: 10.0.0.100        │                           │
│  │  - Port 8081 (REST)             │                           │
│  │  - Port 6123 (RPC)              │                           │
│  │  - Port 6124 (Blob Server)      │                           │
│  └──────────────┬──────────────────┘                           │
│                 │                                               │
│                 │ Internal Communication                        │
│                 │ (RPC, Data Transfer)                          │
│        ┌────────┼────────┬────────────────┐                    │
│        │        │        │                │                     │
│        ▼        ▼        ▼                ▼                     │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐  ┌─────────┐            │
│  │   TM1   │ │   TM2   │ │   TM3   │  │   TM4   │            │
│  │ :6121   │ │ :6121   │ │ :6121   │  │ :6121   │            │
│  │ :6122   │ │ :6122   │ │ :6122   │  │ :6122   │            │
│  └────┬────┘ └────┬────┘ └────┬────┘  └────┬────┘            │
│       │           │           │            │                   │
│       └───────────┴───────────┴────────────┘                  │
│         Data Shuffle Network (All-to-All)                      │
└─────────────────────────────────────────────────────────────────┘
```

---

# 6. Real-world Examples

## Example 1: E-commerce Platform (Mixed Mode)

```
┌────────────────────────────────────────────────────────────────┐
│              E-commerce Company Setup                          │
└────────────────────────────────────────────────────────────────┘

SESSION CLUSTER (Development Team)
┌─────────────────────────────────────┐
│  dev-session-cluster                │
│  Used by: Data Science Team         │
│                                     │
│  Jobs:                              │
│  • Ad-hoc queries                   │
│  • Data exploration                 │
│  • A/B test analysis                │
│  • Quick prototypes                 │
│                                     │
│  Resources: 2 CPU, 8 GB             │
└─────────────────────────────────────┘

APPLICATION MODE (Production)
┌─────────────────────────────────────┐
│  order-stream-processor             │
│  Critical: Real-time orders         │
│  SLA: < 100ms latency               │
│  Resources: 16 CPU, 64 GB           │
│  Replicas: 10 TaskManagers          │
└─────────────────────────────────────┘
          │
          ├─▶ Kafka (orders topic)
          └─▶ PostgreSQL (order DB)

┌─────────────────────────────────────┐
│  fraud-detection                    │
│  Critical: Detect fraud             │
│  SLA: < 500ms latency               │
│  Resources: 8 CPU, 32 GB            │
│  Replicas: 5 TaskManagers           │
└─────────────────────────────────────┘
          │
          ├─▶ Kafka (transactions)
          └─▶ Redis (fraud scores)

┌─────────────────────────────────────┐
│  recommendation-engine              │
│  Medium Priority                    │
│  SLA: < 2s latency                  │
│  Resources: 4 CPU, 16 GB            │
│  Replicas: 3 TaskManagers           │
└─────────────────────────────────────┘
          │
          ├─▶ Clickstream data
          └─▶ Cassandra (recommendations)

┌─────────────────────────────────────┐
│  analytics-aggregator               │
│  Low Priority: Batch stats          │
│  SLA: Daily                         │
│  Resources: 2 CPU, 8 GB             │
│  Replicas: 2 TaskManagers           │
└─────────────────────────────────────┘
          │
          └─▶ Data Warehouse
```

## Example 2: IoT Platform (Application Mode Only)

```
┌────────────────────────────────────────────────────────────────┐
│              IoT Sensor Platform                               │
└────────────────────────────────────────────────────────────────┘

Sensor Data Flow:
10,000 devices → Kafka → Flink → Storage/Alerts

┌─────────────────────────────────────┐
│  sensor-ingestion-pipeline          │
│  • Validates sensor data            │
│  • Enriches with metadata           │
│  • Parallelism: 20                  │
│  • Resources: 20 CPU, 80 GB         │
└─────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  anomaly-detection                  │
│  • ML-based anomaly detection       │
│  • Stateful processing              │
│  • Parallelism: 10                  │
│  • Resources: 16 CPU, 128 GB        │
│    (needs more memory for ML)       │
└─────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  alert-generation                   │
│  • Generates alerts                 │
│  • Sends notifications              │
│  • Parallelism: 5                   │
│  • Resources: 4 CPU, 16 GB          │
└─────────────────────────────────────┘
          │
          ├─▶ PagerDuty
          ├─▶ Slack
          └─▶ Email

Each pipeline is independent and scaled separately!
```

## Example 3: Financial Services (High Availability)

```
┌────────────────────────────────────────────────────────────────┐
│         Financial Trading Platform (Mission Critical)          │
└────────────────────────────────────────────────────────────────┘

Production Region 1 (Primary)
┌─────────────────────────────────────┐
│  trade-processor-primary            │
│  • Processes stock trades           │
│  • Exactly-once semantics           │
│  • Resources: 32 CPU, 256 GB        │
│  • Checkpoints: Every 10 seconds    │
│  • Savepoints: Every 1 hour         │
└─────────────────────────────────────┘
          │
          ├─▶ S3 (checkpoints)
          └─▶ Trade Database

Production Region 2 (Standby)
┌─────────────────────────────────────┐
│  trade-processor-standby            │
│  • Hot standby for failover         │
│  • Same configuration               │
│  • Paused until failover            │
└─────────────────────────────────────┘

Disaster Recovery Process:
1. Primary fails
2. Standby restores from last savepoint
3. Resumes processing (< 60s downtime)
4. Zero data loss (exactly-once)

Feature: Application Mode allows:
✓ Independent scaling
✓ Isolated failures
✓ Resource guarantees
✓ Easy DR setup
```

---

# Summary: When to Use What

## Quick Decision Guide

```
Your Scenario → Recommended Mode

"I'm learning Flink"
└─▶ SESSION MODE

"Running tests in CI/CD"
└─▶ SESSION MODE

"Dev team needs interactive queries"
└─▶ SESSION MODE

"Production streaming pipeline"
└─▶ APPLICATION MODE

"Critical business process"
└─▶ APPLICATION MODE

"Need job isolation"
└─▶ APPLICATION MODE

"Multi-tenant platform"
└─▶ APPLICATION MODE (one per tenant)

"Limited cluster resources, many jobs"
└─▶ SESSION MODE (carefully!)

"SLA requirements, resource guarantees"
└─▶ APPLICATION MODE
```

## Resource Planning

```
SESSION MODE:
• Start small: 2 CPU, 4 GB
• Scale cluster, not jobs
• Monitor slot availability
• Risk: Resource contention

APPLICATION MODE:
• Size per job requirements
• Example job sizes:
  - Small: 2 CPU, 4 GB
  - Medium: 4-8 CPU, 16-32 GB
  - Large: 16+ CPU, 64+ GB
• Independent scaling
• Predictable costs
```
