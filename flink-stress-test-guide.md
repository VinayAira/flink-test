# Flink Stress Test - Complete Guide

## Overview

We'll stress test Flink by:
1. Using StateMachineExample (generates high-volume events)
2. Monitoring throughput, CPU, memory, and latency
3. Gradually increasing load
4. Analyzing performance characteristics

---

## Test Setup

### StateMachineExample Job

This job simulates a state machine with transitions and is perfect for stress testing because:
- ✓ Generates high-volume events (configurable)
- ✓ Stateful processing (tests memory)
- ✓ Complex logic (tests CPU)
- ✓ Tracks metrics (measures latency)

### What It Does

```
┌─────────────────────────────────────────────────────────────┐
│              Event Generator (High Volume)                  │
│  Generates state transition events                          │
│  Rate: 10,000+ events/second (configurable)                │
│                                                             │
│  StateEvent(id, state, timestamp)                          │
│  - ID: Random (creates many parallel states)               │
│  - State: Initial → Processing → Done → Invalid            │
│  - Timestamp: Event time                                    │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              KeyBy (Group by ID)                            │
│  Partition by state machine ID                              │
│  Creates parallel state machines                            │
│                                                             │
│  State Machine 1: Initial → Processing → Done              │
│  State Machine 2: Initial → Processing → Error             │
│  State Machine 3: Initial → Processing → Done              │
│  ...                                                        │
│  State Machine N: ...                                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│           Stateful Processing (Validate)                    │
│  For each state machine:                                    │
│  - Store current state                                      │
│  - Validate transitions                                     │
│  - Detect invalid sequences                                 │
│  - Track metrics                                            │
│                                                             │
│  Valid:   Initial → Processing → Done ✓                   │
│  Invalid: Initial → Done → Processing ✗                   │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                Alert on Errors                              │
│  Generate alerts for:                                       │
│  - Invalid transitions                                      │
│  - Stuck state machines                                     │
│  - Performance issues                                       │
└─────────────────────────────────────────────────────────────┘
```

### State Management

```
Per State Machine:
┌──────────────────────────────────┐
│  State Machine #12345            │
│  ┌────────────────────────────┐  │
│  │ Current State: Processing  │  │
│  │ Previous States: [Initial] │  │
│  │ Timestamp: 1234567890      │  │
│  │ Transition Count: 2        │  │
│  │ Error Count: 0             │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘

Total State Size = Number of Machines × State Size
                 = 10,000 machines × ~500 bytes
                 = ~5 MB (base)

As load increases:
  100,000 machines = ~50 MB
  1,000,000 machines = ~500 MB
  10,000,000 machines = ~5 GB
```

---

## Stress Test Scenarios

### Test 1: Baseline (Low Load)
```yaml
Configuration:
  Parallelism: 2
  Events/sec: 1,000
  State Machines: 1,000
  Expected:
    CPU: < 30%
    Memory: < 200 MB
    Latency: < 10ms
    Throughput: 1,000 events/sec
```

### Test 2: Medium Load
```yaml
Configuration:
  Parallelism: 4
  Events/sec: 10,000
  State Machines: 10,000
  Expected:
    CPU: 50-70%
    Memory: 500 MB - 1 GB
    Latency: 10-50ms
    Throughput: 10,000 events/sec
```

### Test 3: High Load
```yaml
Configuration:
  Parallelism: 8
  Events/sec: 50,000
  State Machines: 50,000
  Expected:
    CPU: 80-90%
    Memory: 2-4 GB
    Latency: 50-200ms
    Throughput: 50,000 events/sec
```

### Test 4: Maximum Load (Backpressure)
```yaml
Configuration:
  Parallelism: 8
  Events/sec: 100,000+
  State Machines: 100,000+
  Expected:
    CPU: 100%
    Memory: 4-6 GB
    Latency: 200ms - 1s+
    Throughput: Limited by resources
    Backpressure: Yes
```

---

## Metrics to Monitor

### 1. Throughput
```
┌─────────────────────────────────────────────────┐
│         Throughput Over Time                    │
│                                                 │
│ Events                                          │
│   │                                             │
│100k│                        ████████            │
│ 80k│                   █████                    │
│ 60k│              █████                         │
│ 40k│         █████                              │
│ 20k│    █████                                   │
│   0└────┴────┴────┴────┴────┴────┴────▶        │
│     0s  10s  20s  30s  40s  50s  60s   Time    │
│                                                 │
│ Observations:                                   │
│ • Linear growth until 40s                      │
│ • Plateau at 100k events/sec (capacity)        │
│ • Indicates maximum throughput reached         │
└─────────────────────────────────────────────────┘
```

**How to measure:**
```bash
# Check Flink metrics
kubectl exec deployment/basic-session-cluster -- curl -s \
  localhost:8081/jobs/<JOB_ID>/metrics?get=numRecordsInPerSecond

# Watch in real-time
watch -n 1 'kubectl exec deployment/basic-session-cluster -- \
  curl -s localhost:8081/jobs/<JOB_ID>/metrics?get=numRecordsInPerSecond'
```

### 2. CPU Utilization
```
┌─────────────────────────────────────────────────┐
│         CPU Usage Over Time                     │
│                                                 │
│  %                                              │
│   │                                             │
│100│                             ████████████    │
│ 80│                       ██████                │
│ 60│                  ████                       │
│ 40│            ████                             │
│ 20│      ████                                   │
│  0└────┴────┴────┴────┴────┴────┴────▶         │
│    0s  10s  20s  30s  40s  50s  60s   Time     │
│                                                 │
│ Observations:                                   │
│ • Steady increase with load                    │
│ • 100% = CPU bound (need more resources)       │
│ • Ideal: 70-80% (room for spikes)             │
└─────────────────────────────────────────────────┘
```

**How to measure:**
```bash
# Monitor CPU usage
kubectl top pods -l app=basic-session-cluster

# Continuous monitoring
watch -n 2 'kubectl top pods -l app=basic-session-cluster'
```

### 3. Memory Consumption
```
┌─────────────────────────────────────────────────┐
│         Memory Usage Over Time                  │
│                                                 │
│  GB                                             │
│   │                                             │
│ 8 │                                  ████       │
│ 6 │                           ██████            │
│ 4 │                    ██████                   │
│ 2 │           ████████                          │
│ 0 └────┴────┴────┴────┴────┴────┴────▶         │
│    0s  10s  20s  30s  40s  50s  60s   Time     │
│                                                 │
│ Observations:                                   │
│ • Grows with state size                        │
│ • Periodic drops = GC events                   │
│ • If flat: state is stable                     │
│ • If growing continuously: memory leak?        │
└─────────────────────────────────────────────────┘
```

**How to measure:**
```bash
# Memory usage
kubectl top pods -l app=basic-session-cluster

# Detailed memory breakdown
kubectl exec <TASKMANAGER_POD> -- \
  curl -s localhost:8081/taskmanagers/<TM_ID>/metrics | \
  grep -i memory
```

### 4. Latency
```
┌─────────────────────────────────────────────────┐
│         Processing Latency                      │
│                                                 │
│  ms                                             │
│   │                                             │
│500│                                      ██     │
│400│                                  ████       │
│300│                             ████            │
│200│                       ██████                │
│100│           ████████████                      │
│  0└────┴────┴────┴────┴────┴────┴────▶         │
│   0%  20%  40%  60%  80%  100% 120%   Load     │
│                                                 │
│ Observations:                                   │
│ • Low at normal load (< 100%)                  │
│ • Spikes when overloaded (> 100%)              │
│ • Backpressure causes latency increase        │
└─────────────────────────────────────────────────┘
```

**How to measure:**
```bash
# End-to-end latency
kubectl exec deployment/basic-session-cluster -- \
  curl -s localhost:8081/jobs/<JOB_ID>/metrics?get=latency
```

---

## Performance Analysis

### Scenario 1: Normal Operation
```
Load: 50% of capacity

┌──────────────────────────────────────┐
│ CPU:    50-60%  ✓ Good               │
│ Memory: Stable  ✓ No leaks           │
│ Latency: 10ms   ✓ Fast               │
│ Throughput: Linear ✓ Scaling well    │
└──────────────────────────────────────┘

State:
┌────────────────┐
│ Input Rate     │ 10,000 events/sec
│ Processing     │ 10,000 events/sec
│ Backpressure   │ None
└────────────────┘

Conclusion: System healthy, can handle more load
```

### Scenario 2: Approaching Capacity
```
Load: 80-90% of capacity

┌──────────────────────────────────────┐
│ CPU:    85%     ⚠ High                │
│ Memory: Growing ⚠ Watch GC            │
│ Latency: 50ms   ⚠ Increasing          │
│ Throughput: Slowing ⚠ Near limit      │
└──────────────────────────────────────┘

State:
┌────────────────┐
│ Input Rate     │ 45,000 events/sec
│ Processing     │ 43,000 events/sec
│ Backpressure   │ Starting
└────────────────┘

Conclusion: Near capacity, consider scaling
```

### Scenario 3: Overloaded
```
Load: > 100% of capacity

┌──────────────────────────────────────┐
│ CPU:    100%    ✗ Maxed out          │
│ Memory: Full    ✗ GC thrashing       │
│ Latency: 500ms+ ✗ Very slow          │
│ Throughput: Declining ✗ Backpressure │
└──────────────────────────────────────┘

State:
┌────────────────┐
│ Input Rate     │ 100,000 events/sec
│ Processing     │ 40,000 events/sec (dropped!)
│ Backpressure   │ SEVERE
└────────────────┘

Conclusion: MUST scale immediately!
```

---

## Load vs Performance Curves

### CPU Utilization Curve
```
CPU %
100 │                           ████████████
 90 │                      ████
 80 │                 ████
 70 │            ████
 60 │       ████
 50 │   ████
 40 │███
  0 └─────────────────────────────────────▶
    0    20k   40k   60k   80k   100k  Events/sec

Zones:
  0-40k:  Green  (Comfortable)
 40-70k:  Yellow (Moderate)
 70-90k:  Orange (High)
 90k+:    Red    (Critical)
```

### Memory Consumption Curve
```
Memory (MB)
6000│                                ████
5000│                           ████
4000│                      ████
3000│                 ████
2000│            ████
1000│       ████
 500│   ████
   0└─────────────────────────────────────▶
    0    20k   40k   60k   80k   100k  Events/sec

Observations:
• Linear growth with state size
• Checkpoint size also grows
• RocksDB uses disk for large state
```

### Latency Curve
```
Latency (ms)
1000│                                  ███
 500│                              ███
 200│                          ████
 100│                      ████
  50│                  ████
  20│              ████
  10│          ████
   0└─────────────────────────────────────▶
    0    20k   40k   60k   80k   100k  Events/sec

Key Point: Latency increases exponentially
when approaching capacity!
```

### Throughput vs Load
```
Throughput (events/sec)
100k│              ████████████████████
 80k│         ████
 60k│     ████
 40k│  ███
 20k│██
   0└─────────────────────────────────────▶
    0    20k   40k   60k   80k   100k  Input Rate

Saturation Point: ~90k events/sec
Beyond this: Backpressure, queuing, drops
```

---

## Bottleneck Analysis

### CPU Bound
```
Symptoms:
✗ CPU at 100%
✓ Memory usage normal
✗ High processing latency
✓ Network not saturated

Diagnosis:
┌──────────────────────────────────┐
│ Task is computationally heavy    │
│ • Complex logic                  │
│ • Heavy serialization            │
│ • Expensive operations           │
└──────────────────────────────────┘

Solutions:
1. Increase parallelism
2. Add more TaskManagers
3. Optimize algorithm
4. Use more efficient serialization
```

### Memory Bound
```
Symptoms:
✓ CPU usage normal
✗ Memory at limit
✗ Frequent GC pauses
✗ OOM errors

Diagnosis:
┌──────────────────────────────────┐
│ State size too large for memory  │
│ • Large keyed state              │
│ • Long windows                   │
│ • Memory leaks                   │
└──────────────────────────────────┘

Solutions:
1. Use RocksDB state backend (disk)
2. Add more memory
3. Reduce state size (TTL, compaction)
4. Increase parallelism (distribute state)
```

### Network Bound
```
Symptoms:
✓ CPU usage low
✓ Memory usage normal
✗ High shuffle cost
✗ Network saturation

Diagnosis:
┌──────────────────────────────────┐
│ Too much data shuffling          │
│ • Heavy keyBy operations         │
│ • Large broadcast variables      │
│ • Inefficient partitioning       │
└──────────────────────────────────┘

Solutions:
1. Optimize partitioning
2. Use local aggregations
3. Compress shuffle data
4. Better network infrastructure
```

### Backpressure
```
Symptoms:
✗ Input > Output
✗ Growing queues
✗ Increasing latency
✗ Source slowing down

Diagnosis:
┌──────────────────────────────────┐
│ Downstream slower than upstream  │
│ • Slow operator                  │
│ • Insufficient resources         │
│ • External system bottleneck     │
└──────────────────────────────────┘

Visualization:
Source → [Queue Growing] → Slow Operator
100k/s     [Buffering]      50k/s

Solutions:
1. Scale up slow operator
2. Optimize slow operator
3. Add parallelism
4. Use async I/O for external calls
```

---

## Checkpoint Impact

### Without Checkpointing
```
Throughput: 100k events/sec (steady)
CPU: 70% (steady)
Latency: 20ms (steady)

Timeline:
│████████████████████████████████████│
0s              30s              60s
   Steady performance
```

### With Checkpointing (Every 10s)
```
Throughput: Variable (dips during checkpoints)
CPU: Spikes to 90% during checkpoints
Latency: Spikes to 100ms during checkpoints

Timeline:
│███████▼███████▼███████▼███████▼███│
0s    10s    20s    30s    40s    50s
      ▲       ▲       ▲       ▲
   Checkpoint points (performance dip)

Impact:
- Small pause (100-500ms)
- Higher CPU (serialization)
- Higher disk I/O
- Increased memory (snapshot)
```

### Checkpoint Size vs Performance
```
State Size    Checkpoint Time    Impact
─────────────────────────────────────────
100 MB        2 seconds          Low
500 MB        10 seconds         Medium
1 GB          20 seconds         High
5 GB          60+ seconds        Severe

Larger state = Longer checkpoints = More overhead
```

---

## Real-World Example: E-commerce

### Scenario: Black Friday Sale

```
Normal Day:
├─ Orders: 1,000/sec
├─ CPU: 30%
├─ Memory: 2 GB
└─ Latency: 10ms ✓

Black Friday Peak:
├─ Orders: 50,000/sec (50x increase!)
├─ CPU: 95%
├─ Memory: 8 GB (near limit)
└─ Latency: 200ms ⚠

Actions Taken:
1. Scale TaskManagers: 4 → 20
2. Increase parallelism: 8 → 40
3. Add memory: 4GB → 8GB per TM

Result:
├─ Orders: 50,000/sec ✓
├─ CPU: 65%
├─ Memory: 4 GB
└─ Latency: 15ms ✓

Cost:
- 5x more resources
- But handled 50x more load
- Total: 10x better efficiency!
```

---

## Optimization Strategies

### 1. Right-sizing Resources
```
Under-provisioned (Bad):
├─ 2 TaskManagers
├─ 1 CPU, 2 GB each
└─ Result: Constant backpressure ✗

Over-provisioned (Wasteful):
├─ 20 TaskManagers
├─ 8 CPU, 32 GB each
└─ Result: 90% idle resources ✗

Optimally-provisioned (Good):
├─ 8 TaskManagers
├─ 4 CPU, 8 GB each
└─ Result: 70% utilization ✓
```

### 2. Parallelism Tuning
```
Too Low (Bottleneck):
Parallelism = 2
[Task 1] [Task 2]
  100%     100%  ← Both maxed out!

Too High (Overhead):
Parallelism = 100
[Task 1] ... [Task 100]
   5%          5%  ← Scheduling overhead!

Optimal:
Parallelism = 16
[Task 1] ... [Task 16]
  70%         70%  ← Balanced!

Rule of thumb:
Parallelism = Number of CPU cores × 2
```

### 3. State Backend Selection
```
HashMap (Memory):
├─ Speed: ⚡⚡⚡ Fastest
├─ Capacity: Limited by heap
├─ Best for: Small state (< 1 GB)
└─ Cost: Low

RocksDB (Disk):
├─ Speed: ⚡⚡ Fast (with cache)
├─ Capacity: TB+ (disk based)
├─ Best for: Large state (> 1 GB)
└─ Cost: Medium

Choose based on state size!
```

---

## Summary: Performance Characteristics

### Key Takeaways

1. **Throughput is limited by**:
   - CPU (computation)
   - Memory (state size)
   - Network (shuffling)
   - I/O (checkpoints, external systems)

2. **Latency increases when**:
   - System approaches capacity
   - Backpressure occurs
   - GC pauses happen
   - Checkpoints run

3. **Memory grows with**:
   - Number of keys (state machines)
   - Window size
   - State per key
   - Checkpoint frequency

4. **CPU grows with**:
   - Event rate
   - Complexity of logic
   - Serialization overhead
   - Number of operators

### Scaling Guidelines

```
Current Load → Action

< 50%  → Comfortable, no action needed
50-70% → Good utilization
70-85% → Monitor closely, plan scaling
85-95% → Scale soon (add resources)
> 95%  → Scale NOW (critical)
```

---

## Next Steps

1. Run stress test with StateMachineExample
2. Monitor all metrics
3. Identify bottlenecks
4. Scale appropriately
5. Optimize if needed

Let's run the actual test!
