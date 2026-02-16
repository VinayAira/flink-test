# Apache Flink Architecture & Working

## Core Components

```
┌─────────────────────────────────────────────────────────┐
│                    FLINK CLUSTER                        │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │         JobManager (Master/Brain)                │  │
│  │  - Coordinates the job                           │  │
│  │  - Distributes work to TaskManagers              │  │
│  │  - Tracks state and checkpoints                  │  │
│  └──────────────────────────────────────────────────┘  │
│                         │                               │
│           ┌─────────────┴─────────────┐                │
│           │                           │                 │
│  ┌────────▼────────┐        ┌────────▼────────┐       │
│  │  TaskManager 1  │        │  TaskManager 2  │       │
│  │  (Worker)       │        │  (Worker)       │       │
│  │                 │        │                 │       │
│  │  ┌───────────┐ │        │  ┌───────────┐ │       │
│  │  │  Task 1   │ │        │  │  Task 3   │ │       │
│  │  └───────────┘ │        │  └───────────┘ │       │
│  │  ┌───────────┐ │        │  ┌───────────┐ │       │
│  │  │  Task 2   │ │        │  │  Task 4   │ │       │
│  │  └───────────┘ │        │  └───────────┘ │       │
│  └─────────────────┘        └─────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

## Data Flow Example: Word Count

### Input Stream:
```
"hello world" → "hello flink" → "world data" → ...
```

### Flink Processing:
```
1. Source (Read Data)
   ↓
2. Map (Split into words)
   ["hello", "world"] → ["hello", "flink"] → ["world", "data"]
   ↓
3. KeyBy (Group by word)
   hello: [hello, hello]
   world: [world, world]
   flink: [flink]
   data:  [data]
   ↓
4. Count (Aggregate)
   hello: 2
   world: 2
   flink: 1
   data:  1
   ↓
5. Sink (Write Results)
   Output to database, file, or dashboard
```

## The Example Job We Deployed

**State Machine Example** simulates:
- A system that tracks state transitions (like order status)
- Generates events: "Order Created" → "Processing" → "Shipped" → "Delivered"
- Processes these events in parallel
- Tracks state and computes statistics

## Key Flink Features

### 1. **Stateful Processing**
Flink remembers information across events:
```
Event 1: User Alice logged in  → State: Alice = online
Event 2: Alice made purchase    → State: Alice = online, purchases = 1
Event 3: Alice logged out       → State: Alice = offline, purchases = 1
```

### 2. **Exactly-Once Processing**
Guarantees each event is processed exactly once (no duplicates, no loss)

### 3. **Low Latency**
Processes events in milliseconds

### 4. **Fault Tolerance**
If a TaskManager crashes, Flink:
- Detects the failure
- Restarts the task on another TaskManager
- Recovers state from checkpoints
- Continues processing (no data loss!)

## When to Use Flink?

✅ **Use Flink when:**
- You need real-time processing (millisecond latency)
- You have continuous data streams (logs, sensors, clicks)
- You need exactly-once guarantees
- You have complex stateful computations

❌ **Don't use Flink when:**
- Batch processing is enough (use Spark or Hadoop)
- Simple data pipelines (use Kafka Streams or cloud functions)
- Very simple transformations (use message queues)

## Flink vs Other Technologies

| Feature          | Flink        | Spark Streaming | Kafka Streams |
|------------------|--------------|-----------------|---------------|
| Processing Model | True Streaming| Micro-batches   | Streaming     |
| Latency          | Milliseconds | Seconds         | Milliseconds  |
| Statefulness     | Built-in     | Limited         | Good          |
| Complexity       | Medium-High  | Medium          | Low           |
| Fault Tolerance  | Excellent    | Good            | Good          |

## Real Code Example

```java
// Simple Flink Word Count Job
StreamExecutionEnvironment env =
    StreamExecutionEnvironment.getExecutionEnvironment();

// Read data stream
DataStream<String> text = env.socketTextStream("localhost", 9999);

// Process: split, count, print
DataStream<Tuple2<String, Integer>> wordCounts = text
    .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
        for (String word : line.split(" ")) {
            out.collect(new Tuple2<>(word, 1));
        }
    })
    .keyBy(value -> value.f0)  // Group by word
    .sum(1);                    // Count occurrences

wordCounts.print();  // Output results

env.execute("Word Count Example");
```

## Monitoring in Kubernetes

In your setup:
- **JobManager Pod**: Coordinates the job (brain)
- **TaskManager Pods**: Execute the actual processing (workers)
- **Flink Operator**: Manages Flink deployments on K8s

You can scale TaskManagers up/down based on load!
