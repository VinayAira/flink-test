# Flink Session Mode: TaskManager Behavior & State Management

## Question 1: Why Only JobManager Pod?

### Current State
```
kubectl get pods

NAME                                    READY   STATUS    RESTARTS   AGE
basic-session-cluster-6564484d84-...    1/1     Running   0          1m
flink-kubernetes-operator-...           1/1     Running   0          2h

Only JobManager is running! Where are TaskManagers?
```

### Explanation: Native Kubernetes Mode

In **Native Kubernetes Mode** (which you're using), Flink uses **dynamic resource allocation**:

```
Session Cluster Lifecycle:

Step 1: Cluster Starts
┌────────────────────────────────────┐
│  JobManager Pod                    │
│  - Started immediately             │
│  - Waiting for jobs                │
│  - NO TaskManagers yet!            │
└────────────────────────────────────┘

Step 2: Job Submitted
┌────────────────────────────────────┐
│  User submits job                  │
│  Required parallelism: 4           │
│  Required slots: 4                 │
└────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────┐
│  JobManager analyzes requirements  │
│  - Need 4 slots                    │
│  - Each TM has 2 slots             │
│  - Need 2 TaskManagers             │
└────────────────────────────────────┘
          │
          ▼
Step 3: TaskManagers Auto-Created
┌────────────────────────────────────┐
│  Kubernetes creates TM pods        │
│                                    │
│  ┌──────────────┐  ┌──────────────┐│
│  │TaskManager 1 │  │TaskManager 2 ││
│  │  2 slots     │  │  2 slots     ││
│  └──────────────┘  └──────────────┘│
└────────────────────────────────────┘

Step 4: Job Running
┌────────────────────────────────────┐
│  JobManager                        │
├────────────────────────────────────┤
│  TaskManager 1   TaskManager 2     │
│  ┌─────┬─────┐  ┌─────┬─────┐     │
│  │Job1 │Job1 │  │Job1 │Job1 │     │
│  │Task1│Task2│  │Task3│Task4│     │
│  └─────┴─────┘  └─────┴─────┘     │
└────────────────────────────────────┘

Step 5: Job Completes (After some time)
┌────────────────────────────────────┐
│  JobManager (still running)        │
│                                    │
│  TaskManagers may be terminated    │
│  after idle timeout                │
└────────────────────────────────────┘
```

### Yes! TaskManagers Are Created Automatically

**Answer: YES** - When you submit a job, Kubernetes will automatically create TaskManager pods based on:
1. Job parallelism requirements
2. Number of slots per TaskManager
3. Resource availability

---

## Question 2: How Flink Stores State

### State Storage Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Flink State Management                  │
└────────────────────────────────────────────────────────────┘

Local State (In-Memory + RocksDB)
┌──────────────────────────────────────┐
│  TaskManager Pod                     │
│  ┌────────────────────────────────┐  │
│  │  Task 1                        │  │
│  │  ┌──────────────────────────┐  │  │
│  │  │  Working State           │  │  │
│  │  │  (Heap Memory/RocksDB)   │  │  │
│  │  │                          │  │  │
│  │  │  counter: 1000           │  │  │
│  │  │  window: [data...]       │  │  │
│  │  └──────────────────────────┘  │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
          │
          │ Periodically checkpoint
          ▼
Persistent Storage (Durable)
┌──────────────────────────────────────┐
│  External Storage (S3/HDFS/NFS)      │
│  ┌────────────────────────────────┐  │
│  │  Checkpoint 1  (t=0s)          │  │
│  │  All state snapshot            │  │
│  ├────────────────────────────────┤  │
│  │  Checkpoint 2  (t=10s)         │  │
│  │  All state snapshot            │  │
│  ├────────────────────────────────┤  │
│  │  Checkpoint 3  (t=20s)         │  │
│  │  All state snapshot            │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
```

### State Backend Types

```
1. HashMap State Backend (Default)
┌──────────────────────────────────┐
│  TaskManager JVM Heap            │
│  ┌────────────────────────────┐  │
│  │  State stored in memory    │  │
│  │  Fast but limited by heap  │  │
│  │  Size: MB to few GB        │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
Use case: Small state, fast access

2. RocksDB State Backend (Recommended)
┌──────────────────────────────────┐
│  TaskManager                     │
│  ┌────────────────────────────┐  │
│  │  RocksDB (Local Disk)      │  │
│  │  State on disk + cached    │  │
│  │  Size: GB to TB            │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
Use case: Large state, production
```

### Checkpointing Process

```
Timeline of Checkpointing:

t=0s: Normal Processing
┌─────────────────────────────────────┐
│  Job Processing Events              │
│  State being updated continuously   │
└─────────────────────────────────────┘

t=10s: Checkpoint Triggered
┌─────────────────────────────────────┐
│  JobManager: "Create checkpoint!"  │
└─────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Checkpoint Barriers sent to tasks  │
│                                     │
│  Source → Barrier → Operators      │
└─────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Each task snapshots its state      │
│  ┌───────────┐  ┌───────────┐      │
│  │  Task 1   │  │  Task 2   │      │
│  │  State    │  │  State    │      │
│  └─────┬─────┘  └─────┬─────┘      │
│        │              │             │
│        └──────┬───────┘             │
└───────────────┼─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│  Write to External Storage          │
│  s3://bucket/checkpoints/chk-123/   │
│  - Task1 state                      │
│  - Task2 state                      │
│  - Metadata                         │
└─────────────────────────────────────┘

t=10.5s: Checkpoint Complete
┌─────────────────────────────────────┐
│  Continue normal processing         │
│  Checkpoint 123 is now available    │
│  for recovery                       │
└─────────────────────────────────────┘
```

---

## Question 3: Handling Pod Failures

### Scenario: TaskManager Pod Dies

```
Before Failure:
┌─────────────────────────────────────────┐
│  JobManager                             │
├─────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐  │
│  │TaskManager 1 │    │TaskManager 2 │  │
│  │ Running ✅   │    │ Running ✅   │  │
│  │              │    │              │  │
│  │ State:       │    │ State:       │  │
│  │ counter=500  │    │ counter=800  │  │
│  └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────┘
Last checkpoint: t=10s (state saved)

Failure Event:
┌─────────────────────────────────────────┐
│  ⚠️  TaskManager 2 Pod Crashes          │
│     (Out of memory / Node failure)      │
└─────────────────────────────────────────┘

Detection (Within seconds):
┌─────────────────────────────────────────┐
│  JobManager detects failure             │
│  "TaskManager 2 is not responding!"     │
└─────────────────────────────────────────┘
          │
          ▼
Recovery Process:
┌─────────────────────────────────────────┐
│  Step 1: Cancel all running tasks      │
│  Step 2: Request new TaskManager       │
│  Step 3: Wait for pod to start         │
│  Step 4: Restore from checkpoint       │
└─────────────────────────────────────────┘

After Recovery:
┌─────────────────────────────────────────┐
│  JobManager                             │
├─────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐  │
│  │TaskManager 1 │    │TaskManager 3 │  │
│  │ Restored ✅  │    │ NEW POD ✅   │  │
│  │              │    │              │  │
│  │ State:       │    │ State:       │  │
│  │ counter=500  │    │ counter=800  │  │
│  │ (from chkpt) │    │ (from chkpt) │  │
│  └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────┘
Resumed from last checkpoint!
Only 10s of work is replayed (since last checkpoint)
```

### Exactly-Once Guarantee

```
Event Stream Processing with Failure:

Input Events: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10
              └─────────┘       │       └──────┘
                  ✅           ⚠️          ?
              Processed     Failure    Unknown

Checkpoint at Event 5:
┌──────────────────────────────────┐
│  State saved after processing:   │
│  Events: 1, 2, 3, 4, 5          │
│  Counter: 5                      │
└──────────────────────────────────┘

Failure occurs at Event 7:
┌──────────────────────────────────┐
│  TaskManager crashes             │
│  Events 6, 7 were in-flight      │
└──────────────────────────────────┘

Recovery:
┌──────────────────────────────────┐
│  Restore from checkpoint         │
│  State: counter=5                │
│  Resume from Event 6             │
│                                  │
│  Reprocess: 6 → 7 → 8 → 9 → 10  │
│  Final counter: 10 ✅           │
└──────────────────────────────────┘

Result: Every event processed exactly once!
```

### Configuration for Fault Tolerance

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: fault-tolerant-job
spec:
  image: flink:1.20
  flinkVersion: v1_20
  flinkConfiguration:
    # Enable checkpointing
    execution.checkpointing.interval: "10s"
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.timeout: "5min"

    # State backend configuration
    state.backend: rocksdb
    state.checkpoints.dir: "s3://my-bucket/checkpoints"
    state.savepoints.dir: "s3://my-bucket/savepoints"

    # Restart strategy
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: "3"
    restart-strategy.fixed-delay.delay: "10s"

    # TaskManager slots
    taskmanager.numberOfTaskSlots: "2"

  job:
    jarURI: local:///app/my-job.jar
    parallelism: 4
```

---

## Complete Failure Handling Timeline

```
t=0s: Job Running Normally
┌────────────────────────────────┐
│ Processing events...           │
│ State: healthy                 │
└────────────────────────────────┘

t=10s: Checkpoint 1 Complete
┌────────────────────────────────┐
│ State saved to S3              │
│ Safe recovery point established│
└────────────────────────────────┘

t=15s: TaskManager Pod Crashes
┌────────────────────────────────┐
│ ⚠️  Pod terminated              │
│ Reason: OOMKilled              │
└────────────────────────────────┘

t=15.1s: JobManager Detects Failure
┌────────────────────────────────┐
│ Heartbeat timeout              │
│ Mark TaskManager as failed     │
└────────────────────────────────┘

t=15.2s: Request New TaskManager
┌────────────────────────────────┐
│ Ask Kubernetes for new pod     │
└────────────────────────────────┘

t=15.5s: New Pod Starting
┌────────────────────────────────┐
│ Container creating...          │
│ Pulling image...               │
└────────────────────────────────┘

t=20s: New Pod Ready
┌────────────────────────────────┐
│ TaskManager started            │
│ Registered with JobManager     │
└────────────────────────────────┘

t=20.5s: Restore from Checkpoint
┌────────────────────────────────┐
│ Download checkpoint from S3    │
│ Restore state (from t=10s)     │
└────────────────────────────────┘

t=25s: Job Resumed
┌────────────────────────────────┐
│ Reprocess events from t=10s    │
│ Continue normal operation      │
└────────────────────────────────┘

Total Downtime: ~10 seconds
Data Loss: ZERO (exactly-once guarantee)
Events Replayed: Only from last checkpoint
```

---

## Summary

### Q1: Will TaskManagers be created automatically?
**YES!** In Native Kubernetes mode:
- JobManager starts immediately
- TaskManagers are created on-demand when jobs are submitted
- Kubernetes dynamically allocates pods based on job requirements

### Q2: How does Flink store state?
- **Working state**: In TaskManager memory/disk (RocksDB)
- **Durable state**: Periodic checkpoints to external storage (S3/HDFS/NFS)
- **Frequency**: Configurable (default: every 10 seconds)
- **Location**: Configured via `state.checkpoints.dir`

### Q3: How are pod failures handled?
1. **Detection**: JobManager detects within seconds
2. **Recovery**: Request new pod from Kubernetes
3. **Restoration**: Load state from last successful checkpoint
4. **Resume**: Reprocess events from checkpoint onwards
5. **Guarantee**: Exactly-once processing (no duplicates, no loss)

### Key Takeaway
```
Flink's fault tolerance is designed for failures:
- Checkpoints = "Save game" feature
- Pod dies? → Load last save
- Continue from where you left off
- Zero data loss!
```
