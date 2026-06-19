# Building an Event-Native Quantitative Infrastructure Ecosystem

*A technical case study on bridging the gap between quantitative research and high-frequency live execution.*

---

## 🛑 The Problem: Siloed Divergence
In algorithmic trading, the most expensive bugs don't crash the system—they silently bleed alpha. This is caused by **siloed divergence**: Quantitative Researchers build alpha signals in Python (using clean CSVs), while Software Engineers rewrite those signals in C++/Java for the live execution stream. 

Because the dev environments differ from production, the signals inevitably diverge. A strategy that backtests beautifully in Jupyter ends up losing money in live markets due to microscopic software calculation skews, clock drift, or out-of-order sequencing.

## 💡 The Solution: Absolute Parity
To solve this, I engineered **The Muninn Ecosystem**—a local-first, event-native infrastructure stack designed to guarantee bit-for-bit parity between historical backtesting and live market execution. 

If a strategy makes money in the simulator, the exact same compiled bytecode handles the live data stream. Divergence is eliminated at the architectural level.

---

## 🏗️ The Three-Pillar Architecture

The ecosystem strictly segregates "memory" (data storage and feature computation), "thought" (strategy execution), and "research" (alpha discovery).

### 1. Muninn (The Data Substrate)
* **Role**: The foundational infrastructure layer.
* **Architecture**: Java 21, Spring Boot, Redpanda, DuckDB.
* **Mechanism**: Muninn ingests raw websocket L3 market data and converts them into immutable, append-only domain events. It computes stateful quantitative features (like *Order Book Imbalance*, *Micro-Price*, and *VPIN*) and streams them in real-time to Redpanda topics, while concurrently rolling cold-storage Parquet files into an Iceberg catalog for historical querying.

### 2. Huginn (The Strategy Engine)
* **Role**: The high-performance execution companion.
* **Architecture**: Go 1.23, Kafka-Go, Prometheus.
* **Mechanism**: Designed as a separate microservice, Huginn acts on Muninn's data. It subscribes to live feature topics, applies quantitative thresholds (e.g., mean-reversion or momentum breakouts), and executes paper-trades. It reconstructs a thread-safe portfolio state and maintains a persistent JSONL trade journal equipped with max-drawdown circuit breakers.

### 3. Muninn-Py (The Research SDK)
* **Role**: The quantitative gateway.
* **Architecture**: Python, Polars, PyArrow.
* **Mechanism**: A zero-config Python library allowing quants to fetch deterministic features directly into Polars DataFrames inside Jupyter Notebooks. Because researchers pull from Muninn's cold-storage Parquet snapshot logs, they can backtest signals with 100% confidence that no lookahead bias exists.

---

## ⚡ Engineering Highlights (Mechanical Sympathy)

Building an infrastructure capable of handling high-frequency market data on a local-first memory budget required aggressive JVM optimization:

* **Zero-Allocation L3 Order Book**: Instead of thrashing the JVM Garbage Collector with rapid object instantiation during order modifications, I implemented an intrusive linked-list L3 Order Book using static, pre-allocated node pools.
* **Contended Memory Padding**: Built a custom Single-Producer Single-Consumer (`SPSCRingBuffer`) for thread-boundary handoffs, applying manual 128-byte cache-line padding to eliminate false sharing and maintain ultra-low latency.
* **Deterministic Event Sourcing**: Guaranteed that any computation over a stream produces identical results when replayed. Live and replay paths share the *exact same* feature-computation logic.

## 🚀 Impact & Takeaways
This ecosystem mirrors the architecture of top-tier proprietary trading firms. By completely decoupling the feature data bus (Muninn) from the stateful strategy engine (Huginn) and the research interface (Muninn-Py), the platform achieves a highly scalable, observable, and strictly deterministic pipeline.

**Explore the Code:**
- [Muninn: The Java Feature Engine](https://github.com/lgreene03/muninn)
- [Huginn: The Go Strategy Executor](https://github.com/lgreene03/huginn)
- [Sleipnir: The Go Execution Gateway](https://github.com/lgreene03/sleipnir)
- [Muninn-Py: The Python SDK](https://github.com/lgreene03/muninn-py)
- [Norse Stack: Meta-Repo & Orchestration](https://github.com/lgreene03/norse-stack)

---
*Built as a showcase of modern, low-latency, and event-native systems engineering.*
