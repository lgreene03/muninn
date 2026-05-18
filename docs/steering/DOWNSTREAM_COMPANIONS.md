# DOWNSTREAM_COMPANIONS.md — Downstream Platform Integrations

This document outlines the architectural blueprints and interface contracts for **companion repositories** built on top of Muninn. 

In strict adherence to `NON_GOALS.md`, Muninn is purely the data & feature infrastructure layer (the "memory"). Actionable execution (trading, paper-trading, interactive research) belongs in separate, downstream projects.

---

## Downstream Platform Architecture

```
                       +------------------------------------+
                       |           Muninn Core              |
                       +------------------------------------+
                                 /                \
                    (Real-Time Redpanda)     (Cold Query API)
                               /                    \
  +---------------------------------------+      +---------------------------------------+
  |        Companion Repo 1: HUGINN       |      |      Companion Repo 2: MUNINN-PY      |
  |     (Quantitative Strategy Engine)    |      |         (Python / Jupyter SDK)        |
  +---------------------------------------+      +---------------------------------------+
  |  - Stateful Strategy Evaluator        |      |  - Zero-Config Polars/Pandas client   |
  |  - Mock Paper-Trading Simulator       |      |  - Direct Trino/DuckDB connectors     |
  |  - Execution Telemetry Logger         |      |  - Notebook Visualization Templates   |
  +---------------------------------------+      +---------------------------------------+
```

---

## 1. Companion Repo 1: Huginn (The Strategy Engine)

### Core Responsibility
**Huginn** (named after Odin's second raven, meaning "thought") is the execution companion. It consumes real-time features emitted by Muninn, evaluates quantitative signals, and simulates paper-trading execution.

### Architectural Flow
1.  **Ingestion**: Subscribes to Muninn's live Redpanda feature topics (e.g., `features.vwap.v1`, `features.obi.v1`).
2.  **Signal Evaluation**: Evaluates threshold logic (e.g., "If Order Book Imbalance (OBI) > 0.8, trigger paper-buy").
3.  **Simulated Execution**: Reconstructs a mock portfolio, tracks slippage and transaction costs, and records paper-trades.
4.  **Telemetry**: Publishes mock trade executions to `events.paper_trade` for visualization.

### Key Downstream Contracts
*   **Feature Consumer Contract**: Huginn expects computed events to match the sealed `FeatureComputedEvent` structure:
    ```json
    {
      "eventId": "019e3b8e-3a0f-7000-ae46-60ec5696e71e",
      "eventTime": "2026-05-18T18:00:00Z",
      "featureName": "obi",
      "featureVersion": "obi@v1.0.0",
      "instrument": "BTC-USDT",
      "values": {
        "obi": 0.825,
        "levels": 5
      }
    }
    ```

---

## 2. Companion Repo 2: Muninn-Py (The Python Research SDK)

### Core Responsibility
**Muninn-Py** is the researcher's gateway. It provides a simple, zero-config Python client allowing quantitative researchers to pull deterministic features directly into Pandas or Polars DataFrames from Jupyter notebooks.

### Architectural Flow
1.  **Connection**: Instantiates a client connecting to Muninn's `query-api` REST server or Trino/DuckDB database directly.
2.  **Metadata Query**: Fetches the list of active features and their schema definitions.
3.  **DataFrame Extraction**: Issues partition-pruned SQL queries to the underlying Iceberg/Parquet catalog, returning highly-optimized Polars dataframes.

### Code Blueprint (The Python API)
```python
import polars as pl
from muninn import MuninnClient

# 1. Connect to Muninn's Query API
client = MuninnClient(host="http://localhost:8080")

# 2. Inspect available signals in the feature registry
features = client.list_features()
print("Available features:", features)

# 3. Pull time-series dataframes for a historical backtest range
df: pl.DataFrame = client.get_features(
    instrument="BTC-USDT",
    features=["obi", "microPrice", "vpin"],
    start="2026-05-18T00:00:00Z",
    end="2026-05-18T12:00:00Z"
)

# 4. Perform alpha evaluation inside Jupyter
print(df.head())
```

---

## Why this Architecture Elevates Your Profile

When presenting this work to quant recruiters (e.g., on LinkedIn or during technical interviews):
1.  **Architecture over "Scripting"**: You demonstrate that you understand how real-world enterprise trading firms segregate their **data transport/feature store (Muninn)** from their **trading/execution engine (Huginn)**.
2.  **API-First Design**: You prove that Muninn's design is decoupled and reusable—allowing a C++/Java execution engine or a Python research sandbox to consume the exact same underlying streaming feed.
3.  **No Lookahead Bias**: Because the feature data is stored deterministically in partition-pruned Parquet files, Python researchers can backtest signals with absolute certainty that no future data leaked into their historical calculations.
