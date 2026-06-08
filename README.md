# Spark Distributed Aggregation Benchmark

> Investigating how aggregation strategy influences scalability, memory utilization, network traffic, and execution performance in distributed data processing systems.

<div align="center">

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-FDEE21?style=for-the-badge&logo=apache&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Big Data](https://img.shields.io/badge/Big%20Data-6A4C93?style=for-the-badge)
![Distributed Systems](https://img.shields.io/badge/Distributed%20Systems-0B5CAD?style=for-the-badge)

</div>

---

## Executive Summary

Distributed systems routinely process billions of records across clusters of machines. While many aggregation approaches can produce identical results, their performance characteristics can differ dramatically when executed at scale.

This project investigates one of the most fundamental optimization concepts in Apache Spark by comparing two aggregation strategies:

- **reduceByKey()** — a scalable, combiner-based aggregation pattern
- **groupByKey()** — a naive aggregation pattern that creates significant shuffle overhead

Using large-scale movie ratings data inspired by the MovieLens ecosystem, the benchmark demonstrates how data movement, shuffle volume, and key distribution directly influence scalability, execution efficiency, and system stability.

The study focuses not only on producing correct results, but on understanding the engineering principles that enable large-scale data processing systems to remain performant under real-world workloads.

---

# Key Achievements

✔ Implemented two distributed aggregation pipelines using Apache Spark

✔ Benchmarked scalable and non-scalable aggregation strategies

✔ Investigated the impact of data skew on distributed workloads

✔ Analyzed shuffle behavior and network transfer overhead

✔ Demonstrated the importance of combiner-based aggregation patterns

✔ Applied MapReduce principles to large-scale movie rating datasets

✔ Evaluated memory and scalability trade-offs in Spark workloads

---

# Why This Project Matters

Every modern technology company depends on distributed aggregation.

Examples include:

- Counting user interactions at Google
- Processing clickstream data at Amazon
- Generating recommendation signals at Netflix
- Computing engagement metrics at Meta
- Aggregating telemetry data from IoT devices
- Monitoring financial transactions at scale

Although aggregation appears simple, inefficient implementations can generate excessive network traffic, memory pressure, and processing bottlenecks.

This project explores why aggregation strategy is often more important than aggregation logic itself.

---

# Problem Statement

Modern data platforms continuously aggregate massive datasets to compute metrics, rankings, and analytical insights.

Common aggregation tasks include:

- Counting events
- Summing transactions
- Calculating averages
- Ranking entities
- Computing engagement metrics

While multiple implementations can produce identical outputs, their execution behavior can vary significantly.

This project investigates two approaches:

## Strategy A — Scalable Aggregation

```python
reduceByKey()
````

### Characteristics

* Performs local pre-aggregation
* Minimizes shuffle volume
* Reduces memory pressure
* Scales efficiently

---

## Strategy B — Naive Aggregation

```python
groupByKey()
```

### Characteristics

* Transfers all records before aggregation
* Creates large intermediate collections
* Increases memory requirements
* Introduces bottlenecks under skewed workloads

Although both methods generate identical results, their scalability characteristics are fundamentally different.

---

# Dataset

## MovieLens Ratings Dataset

The benchmark uses a large-scale movie ratings dataset based on the MovieLens ecosystem.

Each record follows the format:

```text
userId,movieId,rating,timestamp
```

Example:

```text
18,4141,4.5,1425667139
65,208,5.0,1188339308
```

The dataset contains millions of rating events and exhibits significant data skew caused by a small number of highly popular movies receiving a disproportionately large share of ratings.

This characteristic closely resembles real-world production systems where a small subset of keys dominates workload distribution.

---

# Objectives

The project was designed to answer the following engineering questions:

* How does Spark perform large-scale aggregations?
* Why does reduceByKey scale efficiently?
* Why does groupByKey create bottlenecks?
* How does data skew impact distributed workloads?
* How does shuffle volume influence performance?
* What design patterns enable scalable distributed systems?

---

# Methodology

## Part 1 — Scalable Aggregation using reduceByKey()

The first implementation uses Spark's combiner-based aggregation mechanism.

```python
reduceByKey()
```

Workflow:

```text
Ratings Dataset
        ↓
Map Phase
        ↓
Local Aggregation
        ↓
Shuffle
        ↓
Final Aggregation
        ↓
Top Rated Movies
```

This approach aggregates records locally before transferring data across workers.

Benefits include:

* Reduced network traffic
* Smaller shuffle operations
* Lower memory utilization
* Improved scalability

---

## Part 2 — Bottleneck Analysis using groupByKey()

The second implementation intentionally uses a naive aggregation strategy.

```python
groupByKey()
```

Workflow:

```text
Ratings Dataset
        ↓
Map Phase
        ↓
Shuffle ALL Values
        ↓
Large Intermediate Lists
        ↓
Aggregation
```

Unlike reduceByKey, this method transfers every value associated with a key before aggregation occurs.

Consequences include:

* Excessive shuffle traffic
* Increased memory pressure
* Poor scalability
* Hot-key bottlenecks

---

# System Design Perspective

The challenge addressed by this project is not computational complexity.

It is data movement.

In distributed systems, network transfer and memory utilization frequently dominate execution cost.

This benchmark demonstrates two fundamentally different execution philosophies.

## Pre-Aggregation Strategy

```text
Aggregate First
Transfer Less
Scale Better
```

Implemented using:

```python
reduceByKey()
```

---

## Full-Shuffle Strategy

```text
Transfer Everything
Aggregate Later
Create Bottlenecks
```

Implemented using:

```python
groupByKey()
```

The resulting performance gap illustrates why modern distributed systems prioritize local aggregation whenever possible.

---

# Core Engineering Concepts

## Distributed Aggregation

Performing large-scale computations across multiple workers while minimizing communication overhead.

## MapReduce

Applying the Map → Shuffle → Reduce execution paradigm to process large datasets efficiently.

## Data Skew Analysis

Understanding how uneven key distributions create resource imbalance and execution bottlenecks.

## Shuffle Optimization

Investigating how data transfer between workers affects runtime and scalability.

## Memory Management

Analyzing how aggregation strategy impacts intermediate data structures and worker memory consumption.

## Scalability Engineering

Evaluating algorithmic behavior under increasing data volume and workload concentration.

---

# Key Findings

## reduceByKey()

* Performs partial aggregation before shuffle
* Reduces network traffic
* Minimizes memory requirements
* Scales effectively
* Represents the preferred Spark aggregation pattern

### Engineering Insight

Move less data.

Aggregate early.

Scale efficiently.

---

## groupByKey()

* Transfers all records across the network
* Creates large intermediate collections
* Increases memory pressure
* Amplifies skew-related bottlenecks
* Can overwhelm worker resources

### Engineering Insight

Moving raw data is expensive.

Large shuffles create instability.

Scalability suffers.

---

# Results & Insights

The benchmark reveals several important properties of distributed systems.

### Observation 1

Aggregation strategy has a larger impact on scalability than aggregation logic.

Both implementations produce identical outputs while exhibiting dramatically different execution characteristics.

### Observation 2

Network traffic becomes the dominant cost at scale.

Shuffle volume directly impacts runtime, memory consumption, and system stability.

### Observation 3

Data skew amplifies bottlenecks.

Popular entities generate disproportionately large key groups that expose weaknesses in naive aggregation patterns.

### Observation 4

Local aggregation dramatically improves performance.

Reducing intermediate data before shuffle minimizes communication overhead and enables scalable execution.

---

# Real-World Applications

The concepts explored in this project apply directly to:

* Recommendation Systems
* Search Engines
* Customer Analytics Platforms
* Financial Transaction Monitoring
* Marketing Attribution Pipelines
* IoT Telemetry Processing
* Fraud Detection Systems
* Streaming Analytics Platforms
* Log Aggregation Systems

Understanding aggregation performance is essential for any system processing millions or billions of events.

---

# Repository Structure

```text
spark-distributed-aggregation-benchmark/
│
├── part1.py
├── part2.py
├── analysis.txt
├── top_10.txt
└── README.md
```

---


### `requirements.txt`

```txt
pyspark>=3.5.0
```

If you want a slightly more complete version:

```txt
pyspark>=3.5.0
findspark>=2.0.1
```

---
# Reproducing the Experiment

## Prerequisites

Before running the project, ensure the following software is installed:

- Python 3.10+
- Apache Spark 3.x
- Java 8 or higher
- PySpark

Verify Spark installation:

```bash
spark-submit --version
````

---

## Clone Repository

```bash
git clone https://github.com/Astro-Phile/spark-distributed-aggregation-benchmark.git

cd spark-distributed-aggregation-benchmark
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Dataset Setup

Download the MovieLens ratings dataset:

Dataset Source:

[https://www.kaggle.com/datasets/arpit444/ratings](https://www.kaggle.com/datasets/arpit444/ratings)

MovieLens Reference:

[https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/)

Place the ratings file in the project directory:

```text
spark-distributed-aggregation-benchmark/
│
├── ratings.csv
├── part1.py
├── part2.py
└── ...
```

---

## Run Scalable Aggregation Benchmark

Execute the reduceByKey implementation:

```bash
spark-submit part1.py
```

Expected Output:

```text
Top 10 highest-rated movies with at least 50 ratings
```

Output file:

```text
top_10.txt
```

---

## Run Bottleneck Analysis

Execute the groupByKey implementation:

```bash
spark-submit part2.py
```

Depending on machine resources and dataset size, this implementation may:

* Complete successfully
* Execute significantly slower
* Experience memory pressure
* Fail due to skew-induced bottlenecks

This behavior is intentional and forms part of the scalability analysis.

---

## Performance Analysis

While the jobs are running, open the Spark UI:

```text
http://localhost:4040
```

Observe:

* Job execution stages
* Shuffle read/write statistics
* Task execution times
* Memory utilization
* Failed stages (if any)

Use these observations to compare the behavior of reduceByKey and groupByKey.

---

## Expected Learning Outcomes

By reproducing this experiment, you will gain practical insight into:

* Distributed aggregation patterns
* Shuffle optimization
* Data skew effects
* Spark execution internals
* Scalability engineering
* Performance bottleneck analysis


---

# Skills Demonstrated

Apache Spark • PySpark • Distributed Systems • Big Data Engineering • MapReduce • Distributed Aggregation • Scalability Analysis • Performance Benchmarking • Shuffle Optimization • Data Skew Analysis • Memory Analysis • Fault-Tolerant Computing • Analytics Engineering • Performance Diagnostics

---

# Learning Outcomes

This project demonstrates one of the most important lessons in distributed systems engineering:

Producing the correct answer is only part of the challenge.

The real challenge is producing that answer efficiently, reliably, and at scale.

Through the comparison of reduceByKey and groupByKey, the project highlights why combiner-based aggregation patterns are fundamental to modern data platforms and why understanding data movement is often more important than understanding computation itself.

---

# What This Project Demonstrates

This project showcases the ability to:

* Analyze distributed system behavior
* Reason about scalability constraints
* Investigate performance bottlenecks
* Understand Spark execution patterns
* Apply MapReduce principles to large datasets
* Evaluate engineering trade-offs
* Communicate technical findings through structured analysis

Rather than focusing solely on producing correct results, the project emphasizes understanding how algorithmic choices influence performance in large-scale distributed environments.

---

# Author

**Aditya Kashyap**

B.Tech Artificial Intelligence & Data Science
Indian Institute of Technology Jodhpur

Data Analytics • Data Engineering • Machine Learning • Business Intelligence


