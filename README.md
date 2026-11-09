# World Happiness Streaming ETL — Real-Time Predictions with Apache Kafka and Machine Learning

**Course:** ETL (G01) — Data Engineering and Artificial Intelligence, Universidad Autónoma de Occidente (UAO)
**Assignment:** Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning
**Author:** Samuel Arboleda

---

## Table of Contents

1. [Project Description](#1-project-description)
2. [Architecture](#2-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Data Cleaning Decisions](#4-data-cleaning-decisions)
5. [Feature Engineering and Model Training Decisions](#5-feature-engineering-and-model-training-decisions)
6. [Kafka Pipeline and Event Contract](#6-kafka-pipeline-and-event-contract)
7. [Database Schema](#7-database-schema)
8. [Pipeline Results](#8-pipeline-results)
9. [Dashboard](#9-dashboard)
10. [Execution Instructions](#10-execution-instructions)
11. [Limitations and Operational Notes](#11-limitations-and-operational-notes)

---

## 1. Project Description

This project moves from a traditional batch ETL to an **event-driven streaming pipeline** that predicts the **happiness score** of a country in near real time.

The source data are the World Happiness Report files for **2015–2019** (`2015.csv` … `2019.csv`). The five files do **not** share the same schema, so the first part of the project profiles, cleans and harmonizes them into one analytical dataset. A simple **Linear Regression** model is then trained on six explanatory variables (GDP, family/social support, health, freedom, generosity and corruption perception).

In the streaming part, a **Kafka producer** reads historical records one by one and publishes each one as a JSON event. A **Kafka consumer** receives every event, stores it untouched in a raw table, validates it against an explicit **event contract**, applies the same preprocessing used in training, predicts the happiness score with the serialized model, and stores the result in **PostgreSQL**. A **Power BI dashboard** reads the predictions directly from the database.

As the workshop states, the goal is **not** to maximize model accuracy. The focus is a clean architecture, reproducibility, pipeline reliability and data consistency.

**Technology stack:** Python 3.12 · pandas · scikit-learn · Apache Kafka 4.4.0 (Docker) · `confluent-kafka` · PostgreSQL 16 (Docker) · `psycopg2` · Power BI.

---

## 2. Architecture

### Offline process (batch)

```
Historical CSV files (data/raw)
        │
        ▼
Data profiling (EDA)  ──►  reports/*.html, docs/evidence/profiling/*.csv
        │
        ▼
Cleaning + schema harmonization  (etl/cleaning.py)
        │
        ▼
data/processed/happiness_unified.csv   (781 rows × 9 columns)
        │
        ▼
Feature engineering + model training  (notebooks/model_training.ipynb)
        │
        ├──►  models/model.pkl   (StandardScaler + LinearRegression, one sklearn Pipeline)
        └──►  data/streaming/happiness_stream.csv   (240 held-out test rows)
```

### Streaming process

```
data/streaming/happiness_stream.csv
        │
        ▼
Kafka producer  (kafka/producer.py)          key = country, value = JSON event
        │
        ▼
Kafka topic  happiness-events   (3 partitions, replication factor 1)
        │
        ▼
Kafka consumer  (kafka/consumer.py)          group: happiness-ml-team
        │
        ├─ 1. Store the raw message            ──► raw_happiness_events   (status RECEIVED)
        ├─ 2. Validate the event contract      ──► INVALID_SCHEMA / INVALID_VALUES (stored, not predicted)
        ├─ 3. Build features in training order
        ├─ 4. Load model.pkl and predict       ──► PREDICTION_ERROR on failure
        └─ 5. Store the prediction             ──► fact_predictions + dim_country + dim_date (status VALID)
        │
        ▼
PostgreSQL  ──►  Power BI dashboard (4 KPIs)
```

### Infrastructure

Everything runs locally through **Docker Compose**: one single-node Kafka broker (`apache/kafka:4.4.0`, KRaft mode) and one PostgreSQL 16 instance (host port **5433**, persistent volume `postgres_data`).

```yaml
services:
  kafka:
    image: apache/kafka:4.4.0
    container_name: happiness-broker
    ports:
      - "9092:9092"

  postgres:
    image: postgres:16
    container_name: happiness-db
    environment:
      POSTGRES_USER: happiness
      POSTGRES_PASSWORD: happiness
      POSTGRES_DB: happiness
    ports:
      - "5433:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

> The credentials above are for a local development environment only.

---

## 3. Repository Structure

```
.
├── data/
│   ├── raw/                  # original CSV files (read-only, never modified)
│   ├── processed/            # happiness_unified.csv (cleaned dataset)
│   └── streaming/            # happiness_stream.csv (events replayed by the producer)
├── etl/
│   ├── __init__.py
│   └── cleaning.py           # schema harmonization and cleaning logic
├── notebooks/
│   ├── EDA.ipynb             # exploratory data analysis / profiling
│   └── model_training.ipynb  # feature engineering, training, evaluation, model export
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── models/
│   ├── model.pkl             # serialized sklearn Pipeline
│   └── metrics.json          # evaluation metrics of the final model
├── sql/
│   ├── create_tables.sql
│   └── kpis.sql
├── dashboard/                # Power BI file and screenshots
├── docs/evidence/            # profiling tables and feature-engineering figures
├── reports/                  # ydata-profiling HTML reports (one per year)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 4. Data Cleaning Decisions

The EDA (see `notebooks/EDA.ipynb` and `reports/happiness_<year>_profiling.html`) covered missing values, duplicated records, inconsistent column names and data types, schema differences between years and potential outliers. The cleaning logic lives in `etl/cleaning.py` so it is reproducible with a single command (`python etl/cleaning.py`).

### 4.1 Schema differences between years

The same concept is named differently across years, and some columns exist only in some years. Concatenating the files as-is would have produced about 27 sparsely populated columns.

| Unified column | 2015 / 2016 | 2017 | 2018 / 2019 |
|---|---|---|---|
| `country` | `Country` | `Country` | `Country or region` |
| `happiness_score` | `Happiness Score` | `Happiness.Score` | `Score` |
| `gdp` | `Economy (GDP per Capita)` | `Economy..GDP.per.Capita.` | `GDP per capita` |
| `family` | `Family` | `Family` | `Social support` |
| `health` | `Health (Life Expectancy)` | `Health..Life.Expectancy.` | `Healthy life expectancy` |
| `freedom` | `Freedom` | `Freedom` | `Freedom to make life choices` |
| `generosity` | `Generosity` | `Generosity` | `Generosity` |
| `corruption` | `Trust (Government Corruption)` | `Trust..Government.Corruption.` | `Perceptions of corruption` |
| `year` | *(not in the files — added from the file name)* | | |

The unified column names are intentionally the **same names used by the Kafka event contract**, so the producer, the consumer and the SQL tables need no extra translation.

**Unified schema:** `country, year, gdp, family, health, freedom, generosity, corruption, happiness_score`

All data types were standardized (`year` → `int64`, every numeric column → `float64`).

### 4.2 Columns removed and why

| Column(s) | Present in | Reason for removal |
|---|---|---|
| `Happiness Rank` / `Happiness.Rank` / `Overall rank` | all years | Derived from the target: it is the score sorted. **Target leakage.** |
| `Dystopia Residual` / `Dystopia.Residual` | 2015–2017 | The reported score is the sum of the six factors plus this residual, so it contains the answer. **Target leakage.** |
| `Whisker.high`, `Whisker.low` | 2017 | Bounds computed around the score. **Target leakage.** |
| `Lower/Upper Confidence Interval` | 2016 | Bounds computed around the score. **Target leakage.** |
| `Standard Error` | 2015 | Uncertainty of the score itself; only exists in one year. |
| `Region` | 2015–2016 | Not part of the event contract and missing in 60 % of the rows (2017–2019). Filling it by country lookup would invent data not present in the source. |

Two criteria drove these decisions: (1) avoid target leakage, as required by the workshop, and (2) keep only what the model will actually receive in a streaming event.

### 4.3 Country names

`country` is the Kafka event key and feeds `dim_country`, so inconsistent names would turn one country into two. Six naming variants were unified:

| Variants found | Unified name |
|---|---|
| `Hong Kong S.A.R., China` (2017), `Hong Kong` | `Hong Kong` |
| `Taiwan Province of China` (2017), `Taiwan` | `Taiwan` |
| `Trinidad & Tobago` (2018–19), `Trinidad and Tobago` | `Trinidad and Tobago` |
| `Northern Cyprus` (2018–19), `North Cyprus` | `North Cyprus` |
| `Macedonia` (2015–18), `North Macedonia` (2019) | `North Macedonia` |
| `Somaliland region` (2015), `Somaliland Region` (2016) | `Somaliland Region` |

`Somaliland Region` was **not** merged with `Somalia`, and `Sudan` / `South Sudan` remain separate: the report treats them as distinct entities. Other countries that do not appear in every year (for example Gambia, Oman or Belize) reflect the real coverage of each report edition, not naming problems, so no action was taken.

### 4.4 Missing values

Only **one** missing value exists in the five files: `corruption` for **United Arab Emirates in 2018**.

**Decision: the row was removed** (1 of 782 rows, 0.13 %).
- The impact on the model is negligible.
- Imputing a value would put synthetic data into a field the contract defines as the real observation.
- The event contract requires a numeric `corruption` in every event; leaving a null would make that event invalid in the consumer anyway.

### 4.5 Duplicates

No fully duplicated rows and no repeated countries within a year were found. `etl/cleaning.py` asserts that `(country, year)` is unique after harmonization.

### 4.6 Outliers

Outliers were **detected, inspected and kept**. They were identified with the IQR rule (1.5 × IQR):

| Column | Outliers | Who they are |
|---|---|---|
| `family` | 10 | Central African Republic (2015, 2017, 2018, 2019) and Togo (2016), all with value 0 |
| `generosity` | 17 | Mainly Myanmar (2015–2018) and Indonesia (2017), unusually high values |
| `corruption` | 67 | Countries with the best institutional perception (Rwanda, Qatar, Singapore, Denmark…). The variable is right-skewed |
| `gdp`, `health`, `freedom`, `happiness_score` | 0 | — |

**Why they were kept:** none of them is a data-entry error. There are no negative values and no values outside the valid scales, the extremes belong to identifiable countries, and they repeat across years (a real behavior, not a typo). Removing them would discard legitimate information from the most extreme countries.

### 4.7 Valid ranges

| Check | Result |
|---|---|
| Negative values in any variable | none |
| `happiness_score` range | 2.693 – 7.769 (inside the 0–10 scale) |
| Minimum of the six explanatory variables | 0.0 (a zero is a plausible real value: a country at the bottom of the scale) |

These checks define the validation thresholds used later by the consumer (see section 6).

### 4.8 Result

| Stage | Rows |
|---|---|
| Raw files (2015–2019) | 782 (158 + 157 + 155 + 156 + 156) |
| After cleaning | **781** × 9 columns |

The cleaned dataset covers **164 distinct countries**: 145 appear in all five years and 19 in fewer years. It is stored in `data/processed/happiness_unified.csv`.

---

## 5. Feature Engineering and Model Training Decisions

All of this lives in `notebooks/model_training.ipynb`. Figures are saved in `docs/evidence/features/`.

### 5.1 Exploration

| Variable | Correlation with `happiness_score` |
|---|---|
| `gdp` | 0.79 |
| `health` | 0.74 |
| `family` | 0.65 |
| `freedom` | 0.55 |
| `corruption` | 0.40 |
| `generosity` | 0.14 |

The highest correlation **between** explanatory variables is `gdp`–`health` (0.79); the rest are lower (0.59 or below). `family`, `gdp`, `health` and `freedom` are concentrated in high values, while `generosity` and `corruption` are right-skewed.

### 5.2 Decisions

| Decision | Justification |
|---|---|
| **Use the six contract variables** (`gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`) | No variable is redundant (maximum pairwise correlation 0.79), none leaks the target, and all of them arrive in every event. |
| **Exclude `country`** | A 164-value identifier and the Kafka key; a model that learned names would not generalize to new countries. |
| **Exclude `year`** | It does not explain happiness and only covers 2015–2019. |
| **No categorical encoding** | After excluding `country`, every input is numeric. |
| **No transformation of skewed variables** | Linear regression does not require symmetric features, and the workshop asks for a simple model. |
| **Scale with `StandardScaler` inside a `Pipeline`** | Does not change linear-regression predictions, but makes coefficients comparable and, above all, **ships the preprocessing together with the model** in `model.pkl`, so the consumer applies exactly the same transformation as training. |
| **Never use `actual_happiness_score` as an input** | It is the target; in the event it is used only to compute the prediction error. |

### 5.3 Train / test split

A **70 % / 30 %** split was used, **grouped by country** (`GroupShuffleSplit`, `random_state=42`). Each country appears in up to five years with very similar values, so a random split would place the same country in both sets and make the metrics optimistic. Grouping guarantees the model is evaluated on countries it never saw.

| Set | Rows |
|---|---|
| Training | 541 |
| Test | 240 (50 countries) |
| Countries present in both sets | 0 |

### 5.4 Model and results

Model: scikit-learn `Pipeline([StandardScaler, LinearRegression])`.

**Comparison of feature sets** (evaluated on the test set):

| Variables | MAE | RMSE | R² |
|---|---|---|---|
| `gdp` + `health` only | 0.523 | 0.635 | 0.724 |
| Six variables without `generosity` | 0.434 | 0.569 | 0.778 |
| **All six variables (final model)** | **0.432** | **0.562** | **0.784** |

Moving from two to six variables clearly improves the model, which justifies using all of them. `generosity` adds almost nothing (R² +0.006, possibly noise from a single split), but it does no harm and is part of the contract.

**Final model — coefficients** (variables scaled):

| Variable | Coefficient |
|---|---|
| `gdp` | 0.445 |
| `freedom` | 0.242 |
| `family` | 0.240 |
| `health` | 0.208 |
| `corruption` | 0.073 |
| `generosity` | 0.063 |

`health` weighs less than its correlation with the score suggests because it overlaps with `gdp` (0.79), which already absorbs the information they share.

The model is saved as `models/model.pkl` (a `joblib` file) and its metrics in `models/metrics.json`. The notebook also writes the **240 held-out test rows** to `data/streaming/happiness_stream.csv`, sorted by year and country, which is the dataset replayed by the producer. Streaming only unseen rows keeps the dashboard metrics honest: the error shown is the error on countries the model never trained on.

> The `model.pkl` file can only be loaded safely with the **same scikit-learn version** used to create it, so that version is pinned in `requirements.txt`.

---

## 6. Kafka Pipeline and Event Contract

### 6.1 Topic

| Property | Value |
|---|---|
| Name | `happiness-events` |
| Partitions | 3 |
| Replication factor | 1 (single local broker) |
| Event key | `country` |

The 50 countries are distributed among the 3 partitions by Kafka's key hashing. All events of the same country always land in the **same partition**, so their order is preserved. Partitions are not per country: many countries share a partition.

### 6.2 Event contract

Kafka only transports bytes; the application is responsible for interpreting and validating the business fields. Every event must follow this contract:

| Field | Type | Rule |
|---|---|---|
| `event_id` | string | Non-empty, unique identifier (`E001`, `E002`, …) |
| `country` | string | Non-empty; also the Kafka key |
| `year` | integer | Historical observation year |
| `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption` | number | Compatible with the unified training schema |
| `actual_happiness_score` | number | Used **only** for evaluation, never as a model input |
| `event_time` | string | Valid ISO-8601 timestamp of when the producer emitted the event |

Example event:

```json
{
  "event_id": "E001",
  "country": "Algeria",
  "year": 2015,
  "gdp": 0.93929,
  "family": 1.07772,
  "health": 0.61766,
  "freedom": 0.28579,
  "generosity": 0.07822,
  "corruption": 0.17383,
  "actual_happiness_score": 5.605,
  "event_time": "2026-11-07T19:45:42"
}
```

### 6.3 Producer (`kafka/producer.py`)

- Reads `data/streaming/happiness_stream.csv` **record by record** and publishes each as a JSON event.
- Uses the country as the event key.
- Simulates arrival over time with a configurable delay (`--delay`, default 0.5 s). `--limit N` sends only the first N events (useful for tests).
- Renames `happiness_score` to `actual_happiness_score` when building the event.
- `event_time` is the moment of emission (local time, ISO-8601).
- Before sending, it checks that Kafka is reachable and that the topic exists with 3 partitions; otherwise it stops with a clear message instead of letting Kafka auto-create a wrongly configured topic.

### 6.4 Consumer (`kafka/consumer.py`)

Consumer group: **`happiness-ml-team`**. For every message it performs, in this order:

1. **Store the raw message** exactly as received in `raw_happiness_events`, with status `RECEIVED`, *before* any validation, transformation or prediction.
2. **Parse the JSON.** If it is not valid JSON → `INVALID_SCHEMA`.
3. **Validate the schema** → `INVALID_SCHEMA` on failure.
4. **Validate the values** → `INVALID_VALUES` on failure.
5. **Predict** with `model.pkl`, building the feature row in the exact order the model was trained with (`model.feature_names_in_`).
6. **Store the result** (country, date, prediction) and mark the raw event `VALID`. Any failure in prediction or persistence → `PREDICTION_ERROR`.
7. **Commit the Kafka offset** for the message.

**Validation rules**

| Status | Triggered when |
|---|---|
| `INVALID_SCHEMA` | Not valid JSON or not a JSON object; a required field is missing (this also covers any feature the model needs); `event_id`/`country` are not non-empty strings; `year` is not an integer; a numeric field is not a number (booleans rejected); `event_time` is not a valid ISO-8601 timestamp |
| `INVALID_VALUES` | `year` outside 2015–2019; a numeric field is not finite (NaN/Infinity); any of the six features outside **[0, 3]**; `actual_happiness_score` outside **[0, 10]** |
| `PREDICTION_ERROR` | The model fails or returns a non-finite value, or the result cannot be stored |

Invalid records are **stored in the raw table with their status and reason, skipped from prediction, and never crash the pipeline**.

**Reliability design**
- **Manual offset commit** (`enable.auto.commit = False`): an offset is committed only after the message is fully processed. If the consumer crashes mid-event, Kafka redelivers it.
- **Idempotent writes:** the raw table has a unique constraint on `(topic, partition, offset)`, so a redelivered message is not stored twice; an event left in `RECEIVED` is resumed instead of skipped. `fact_predictions.raw_event_id` is also unique.
- **Atomic persistence:** country, date, prediction and the final status are written in a single database transaction.
- **Single source of truth for features:** the feature order comes from the model itself, not from a separate list.

### 6.5 Validation test

The validation was exercised by publishing five hand-made events with Kafka's console producer:

| Event | Problem | Stored status |
|---|---|---|
| `esto no es un json` | Not JSON | `INVALID_SCHEMA` |
| `{"event_id":"T02","country":"Colombia","year":2019}` | Missing fields | `INVALID_SCHEMA` |
| `gdp` sent as the string `"alto"` | Wrong type | `INVALID_SCHEMA` |
| `gdp` = `-1.0` | Invalid value | `INVALID_VALUES` |
| A fully correct event | — | `VALID` (prediction stored) |

The four invalid test events are kept in `raw_happiness_events` as evidence; the valid test prediction was removed so it would not pollute the dashboard.

---

## 7. Database Schema

PostgreSQL 16 (`sql/create_tables.sql`, idempotent: it uses `CREATE TABLE IF NOT EXISTS`). The model is a small star schema: one fact table, two dimensions and a raw/staging table for traceability.

```
raw_happiness_events 1 ──── 1 fact_predictions N ──── 1 dim_country
                                      │
                                      N
                                      │
                                      1
                                  dim_date
```

### `raw_happiness_events` — raw / staging table (not a dimension)

| Column | Type | Description |
|---|---|---|
| `raw_event_id` | BIGSERIAL, PK | Internal identifier |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | TEXT / INTEGER / BIGINT | Where the message came from (**unique together**) |
| `kafka_key` | TEXT | Event key |
| `raw_payload` | TEXT | The original message, unmodified |
| `event_id` | TEXT | Extracted from the message when possible |
| `received_at` | TIMESTAMP | When the consumer received it |
| `processing_status` | TEXT | `RECEIVED`, `VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`, `PREDICTION_ERROR` |
| `status_detail` | TEXT | Reason when the event is invalid or fails |

### `dim_country`

| Column | Type | Description |
|---|---|---|
| `country_id` | SERIAL, PK | |
| `country_name` | TEXT, UNIQUE | One row per country |

### `dim_date`

| Column | Type | Description |
|---|---|---|
| `date_id` | INTEGER, PK | `YYYYMMDD` |
| `full_date` | DATE, UNIQUE | |
| `year`, `quarter`, `month`, `day` | SMALLINT | |

### `fact_predictions`

| Column | Type | Description |
|---|---|---|
| `prediction_id` | BIGSERIAL, PK | |
| `raw_event_id` | BIGINT, UNIQUE, FK | Link to the exact event that produced the prediction |
| `country_id` | INTEGER, FK | → `dim_country` |
| `date_id` | INTEGER, FK | → `dim_date` (date the event was emitted) |
| `observation_year` | SMALLINT | Year of the data point (2015–2019) |
| `actual_score` | NUMERIC(6,3) | Real happiness score |
| `predicted_score` | NUMERIC(6,3) | Model prediction |
| `prediction_error` | NUMERIC(6,3) | `predicted − actual` (signed) |
| `event_time` | TIMESTAMP | When the producer emitted the event |
| `prediction_timestamp` | TIMESTAMP | When the consumer made the prediction |

### Design decisions

- **`raw_payload` is plain text, not JSON.** A damaged event may not be valid JSON, and the raw table must be able to store anything that arrives.
- **Raw table first, with `RECEIVED` status.** The message is persisted before validation so no event is ever lost.
- **Unique Kafka position and unique `raw_event_id` in the fact table** prevent duplicates when a message is redelivered, and keep a strict one-to-one trace from prediction to raw event.
- **`dim_date` is tied to the emission date, and the fact table keeps `observation_year` separately.** The whole producer run happens in a couple of minutes, so a trend over emission time would show a single point. The observation year (2015–2019) gives a meaningful trend.
- **`prediction_error` keeps its sign** to reveal whether the model over- or under-predicts. The error KPI uses `ABS(prediction_error)`, otherwise positive and negative errors would cancel out.
- **Timestamps use the same local clock** (set by the producer and the consumer in Python), so `prediction_timestamp − event_time` is a valid latency measure.

---

## 8. Pipeline Results

Full run: 240 events published by the producer and consumed in real time.

| Processing status | Events |
|---|---|
| `VALID` | 240 |
| `INVALID_SCHEMA` | 3 (hand-made test events) |
| `INVALID_VALUES` | 1 (hand-made test event) |

**Streaming error metrics** (from `fact_predictions`):

| Metric | Value |
|---|---|
| Predictions | 240 |
| Mean absolute error | **0.432** |
| RMSE | **0.562** |
| Mean signed error (bias) | −0.010 |

The streaming MAE and RMSE are **identical** to the offline test metrics (0.432 and 0.562). This is the evidence that the consumer applies exactly the same model, features and preprocessing as the training notebook.

**Error by observation year**

| Year | Predictions | Mean real score | Mean predicted score | Mean absolute error |
|---|---|---|---|---|
| 2015 | 49 | 5.390 | 5.285 | 0.399 |
| 2016 | 48 | 5.386 | 5.106 | 0.495 |
| 2017 | 47 | 5.344 | 5.484 | 0.433 |
| 2018 | 47 | 5.352 | 5.459 | 0.429 |
| 2019 | 49 | 5.347 | 5.439 | 0.406 |

The model does not perform equally every year: 2016 has the highest error and is under-predicted, while 2017–2019 are slightly over-predicted. A possible explanation (a hypothesis, not a verified conclusion) is that each report edition measures the variables slightly differently while the model learned a single relationship across all years.

---

## 9. Dashboard

The dashboard is built in **Power BI** and **queries the PostgreSQL database directly — not CSV files**. The queries that back each KPI are in `sql/kpis.sql`.

| KPI | Source query | Suggested visual |
|---|---|---|
| 1. Average prediction error | KPI 1 — MAE, RMSE, bias and total predictions | Cards |
| 2. Predictions by country | KPI 2 — predictions and mean absolute error per country | Bar chart |
| 3. Predicted vs. actual score | KPI 3 — one row per prediction | Scatter plot (the closer to the diagonal, the better) |
| 4. Prediction trends over time | KPI 4 — metrics per observation year | Line chart (actual vs. predicted mean score) |
| Extra: pipeline reliability | Event counts by processing status | Card / bar |

**Connection settings (Power BI → Get Data → PostgreSQL database):**

| Setting | Value |
|---|---|
| Server | `localhost:5433` |
| Database | `happiness` |
| User / password | `happiness` / `happiness` |

Power BI Desktop may ask you to install the Npgsql provider the first time.

> **Note on KPI definitions:** the "average prediction error" is the **mean absolute error**, because the stored error is signed. The "trend over time" uses the **observation year**, not the emission date (see section 7).

<!-- TODO (before submission): add dashboard screenshots to dashboard/ and reference them here, e.g. ![Dashboard](dashboard/dashboard_overview.png) -->
<!-- TODO (before submission): add the .pbix file (or a shareable link) to dashboard/ and mention it here -->

---

## 10. Execution Instructions

Commands are for **Windows PowerShell**, run from the project root. Prerequisites: **Docker Desktop** (running), **Python 3.12** and Git.

### 1. Environment

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell blocks the activation script, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` first.

### 2. Data

Place `2015.csv` … `2019.csv` in `data/raw/`.

### 3. Clean and unify the data

```powershell
python etl/cleaning.py
```

Prints the checks and writes `data/processed/happiness_unified.csv` (781 rows).

### 4. Train the model

Open `notebooks/model_training.ipynb` with the `.venv` kernel and run all cells. It produces `models/model.pkl`, `models/metrics.json` and `data/streaming/happiness_stream.csv`.
(`notebooks/EDA.ipynb` regenerates the profiling reports in `reports/`; it is not required to run the pipeline.)

### 5. Start the infrastructure

```powershell
docker compose up -d
docker compose ps

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic happiness-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic happiness-events --bootstrap-server localhost:9092

Get-Content -Raw sql\create_tables.sql | docker compose exec -T postgres psql -U happiness -d happiness
docker compose exec postgres psql -U happiness -d happiness -c "\dt"
```

The last command should list the four tables.

### 6. Run the streaming pipeline

Open **two terminals** (with the `.venv` active in both).

**Terminal 1 — consumer** (leave it running):

```powershell
python kafka/consumer.py
```

**Terminal 2 — producer:**

```powershell
python kafka/producer.py --delay 0.2          # all 240 events
python kafka/producer.py --limit 5 --delay 0.2   # quick test with 5 events
```

Stop the consumer with `Ctrl + C`; it prints a summary by status.

### 7. Verify the results

```powershell
docker compose exec postgres psql -U happiness -d happiness -P pager=off -c "SELECT processing_status, COUNT(*) FROM raw_happiness_events GROUP BY processing_status ORDER BY 1;"
docker compose exec postgres psql -U happiness -d happiness -P pager=off -c "SELECT COUNT(*) AS predictions, ROUND(AVG(ABS(prediction_error)), 3) AS mae FROM fact_predictions;"
```

Expected: 240 events `VALID`, and an MAE of about 0.432.

### 8. (Optional) Test the validation with invalid events

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --topic happiness-events --bootstrap-server localhost:9092
```

Paste these lines one at a time (press Enter after each, `Ctrl + C` to finish) while the consumer is running:

```
esto no es un json
{"event_id": "T02", "country": "Colombia", "year": 2019}
{"event_id": "T04", "country": "Colombia", "year": 2019, "gdp": -1.0, "family": 0.8, "health": 0.9, "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "actual_happiness_score": 6.2, "event_time": "2026-11-07T21:30:00"}
```

They should be stored as `INVALID_SCHEMA`, `INVALID_SCHEMA` and `INVALID_VALUES` respectively, without stopping the consumer.

### 9. Dashboard

Open the Power BI file in `dashboard/` and connect it to the database with the settings in [section 9](#9-dashboard). Queries for each KPI are in `sql/kpis.sql`.

### 10. Stop and resume

```powershell
docker compose stop      # pause (keeps Kafka topic and database data)
docker compose start     # resume
```

---

## 11. Limitations and Operational Notes

- **Single train/test split.** The metrics come from one split, so very small differences (for example the contribution of `generosity`) may be noise.
- **Only held-out rows are streamed.** The dashboard shows the error on 240 rows / 50 countries that the model did not train on; it does not cover the full dataset.
- **Uneven error across years.** The model performs worse on 2016 than on other years (section 8).
- **Zero values.** Every explanatory variable has a minimum of 0. They are treated as valid observations (the validation range is `≥ 0`), although a zero could in principle also represent a missing measurement.
- **Timestamps are naive local time.** `event_time`, `received_at` and `prediction_timestamp` are all written with the local clock of the machine running the pipeline, without a time-zone offset.
- **Kafka has no persistent volume.** Running `docker compose down` removes the Kafka container and its topic (the PostgreSQL data survives thanks to its volume); `docker compose down -v` also deletes the database. Use `docker compose stop` to pause.
- **Local credentials.** The database credentials are defaults for local development. The consumer reads `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER` and `DB_PASSWORD` from environment variables when they are defined.
- **Reprocessing.** The consumer commits offsets per consumer group. To replay the topic from the beginning, use a new `group.id` (and clean the tables to avoid duplicated demonstration data).