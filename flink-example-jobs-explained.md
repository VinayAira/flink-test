# Current Running Jobs - Detailed Explanation

## Job 1: CarTopSpeedWindowingExample (TopSpeedWindowing)

### What It Does
Simulates a car racing scenario where multiple cars report their speed continuously, and we track the maximum speed for each car within time windows.

### Data Flow
```
┌─────────────────────────────────────────────────────────────┐
│                    Data Generation                          │
│  Generates random car events every 100ms                    │
│  CarEvent(carId, speed, timestamp)                          │
│                                                             │
│  Example data:                                              │
│  Car-1: 85 mph                                             │
│  Car-2: 92 mph                                             │
│  Car-1: 88 mph                                             │
│  Car-3: 75 mph                                             │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                 KeyBy (Group by Car)                        │
│  Partition events by carId                                  │
│                                                             │
│  Stream 1: Car-1 → 85, 88, 90, 87...                      │
│  Stream 2: Car-2 → 92, 95, 91, 89...                      │
│  Stream 3: Car-3 → 75, 78, 76, 80...                      │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│           Window (Tumbling, 5 seconds)                      │
│  Group events into 5-second windows                         │
│                                                             │
│  Window 1 (t=0-5s):  Car-1 → [85, 88, 90, 87, 89]        │
│  Window 2 (t=5-10s): Car-1 → [91, 93, 92, 90, 88]        │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Aggregation (Max Speed)                        │
│  Calculate maximum speed in each window                     │
│                                                             │
│  Window 1: Car-1 max = 90 mph                              │
│  Window 2: Car-1 max = 93 mph                              │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                    Output/Print                             │
│  Print top speeds:                                          │
│  "Car-1: 90 mph (window 0-5s)"                            │
│  "Car-1: 93 mph (window 5-10s)"                           │
└─────────────────────────────────────────────────────────────┘
```

### Resource Characteristics
```
Throughput:  ~10 events/second (low)
State Size:  Small (current window data only)
CPU Usage:   Low (simple aggregation)
Memory:      Low (~50-100MB)
Latency:     5 seconds (window size)
```

### State Management
```
Per-Key State (each car):
┌──────────────────────────────────┐
│  Car-1 State                     │
│  ┌────────────────────────────┐  │
│  │ Current Window:            │  │
│  │ Events: [85, 88, 90, 87]   │  │
│  │ Max so far: 90             │  │
│  │ Window start: t=0          │  │
│  │ Window end: t=5            │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘

Total State = Number of Cars × Window Data
```

---

## Job 2: WindowJoin Example

### What It Does
Demonstrates joining two streams of events within the same time window - simulating matching user clicks with impressions.

### Data Flow
```
┌─────────────────────────────────────────────────────────────┐
│              Two Data Generators                            │
│                                                             │
│  Stream A (Grades):        Stream B (Salaries):            │
│  Person-1: Grade A         Person-1: $50k                  │
│  Person-2: Grade B         Person-3: $60k                  │
│  Person-3: Grade A         Person-2: $55k                  │
└────────────┬─────────────────────────────┬──────────────────┘
             │                             │
             ▼                             ▼
┌────────────────────────┐   ┌────────────────────────┐
│  KeyBy (Person ID)     │   │  KeyBy (Person ID)     │
│  Stream A              │   │  Stream B              │
└────────────┬───────────┘   └────────────┬───────────┘
             │                             │
             └──────────┬──────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Join Window (2 seconds)                        │
│  Match events from both streams in same window              │
│                                                             │
│  If Person-1 appears in both streams within 2s:            │
│  Join(Grade A, $50k) → Output: Person-1, A, $50k          │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                    Output                                   │
│  "Person-1 has grade A and salary $50k"                   │
└─────────────────────────────────────────────────────────────┘
```

### Join Window Visualization
```
Timeline (2-second windows):

Stream A (Grades):
t=0    t=0.5  t=1    t=1.5  t=2    t=2.5
│      │      │      │      │      │
P1:A   P2:B   P3:A   P1:B   P4:C   P2:A
└──────────────────────────┘
    Window 1 (0-2s)
           └──────────────────────────┘
               Window 2 (1-3s)

Stream B (Salaries):
t=0    t=0.5  t=1    t=1.5  t=2    t=2.5
│      │      │      │      │      │
       P1:50k        P3:60k P2:55k
└──────────────────────────┘
    Window 1 (0-2s)

Matches in Window 1:
✓ P1 (Grade A + $50k)
✓ P3 (Grade A + $60k)
✗ P2 (Grade B only, no salary in window)
```

### Resource Characteristics
```
Throughput:  ~6 events/second (3 per stream)
State Size:  Medium (buffering both streams)
CPU Usage:   Medium (join operations)
Memory:      Medium (~100-200MB)
Latency:     2 seconds (window size)
```

### State Management
```
Per-Key State (each person):
┌──────────────────────────────────┐
│  Person-1 State                  │
│  ┌────────────────────────────┐  │
│  │ Window Buffer A:           │  │
│  │ [Grade A at t=0.1]         │  │
│  │                            │  │
│  │ Window Buffer B:           │  │
│  │ [Salary $50k at t=0.5]     │  │
│  │                            │  │
│  │ Waiting for window close   │  │
│  │ to emit join result        │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘

Memory Usage = Number of Keys × (Buffer A + Buffer B)
               in current windows
```

---

## Performance Comparison

```
┌────────────────────┬──────────────┬──────────────┐
│     Metric         │   Job 1      │   Job 2      │
├────────────────────┼──────────────┼──────────────┤
│ Events/sec         │     ~10      │     ~6       │
├────────────────────┼──────────────┼──────────────┤
│ State Size         │    Small     │   Medium     │
├────────────────────┼──────────────┼──────────────┤
│ CPU Usage          │     Low      │   Medium     │
├────────────────────┼──────────────┼──────────────┤
│ Memory Usage       │   50-100MB   │  100-200MB   │
├────────────────────┼──────────────┼──────────────┤
│ Latency            │   5 seconds  │  2 seconds   │
├────────────────────┼──────────────┼──────────────┤
│ Complexity         │    Simple    │   Complex    │
└────────────────────┴──────────────┴──────────────┘
```

## Why These Are NOT Good Stress Tests

### Limitations:

1. **Low Throughput**
   - Only generating ~10-20 events/second
   - Real systems: thousands to millions events/sec

2. **Small State**
   - Only storing current window data
   - Real systems: GB to TB of state

3. **Simple Operations**
   - Basic aggregations (max, join)
   - Real systems: complex CEP, ML, enrichment

4. **Fixed Load**
   - No ability to increase load
   - Can't test scaling behavior

5. **No Backpressure Testing**
   - Always processing in real-time
   - Can't test what happens under overload

---

## What We Need for Stress Testing

To properly test Flink, we need:

```
✓ High event rate (10k-100k+ events/sec)
✓ Large state (GB+)
✓ Complex operations
✓ Configurable load
✓ Metrics collection
✓ Backpressure simulation
✓ Memory stress
✓ CPU stress
```

Let's create a proper stress test next!
