# Kafka Topics — UKWA Crawling Pipeline

## Overview

The UKWA crawling pipeline uses Apache Kafka as the messaging backbone between its main components: the **Heritrix crawler**, the **scoper service**, and downstream consumers. Topics carry URLs through distinct pipeline stages — from candidate discovery through scoping, crawling, and result logging.

Auto-creation of topics is disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`). All topics are pre-created at startup with **16 partitions**, **replication factor 1**, and **snappy compression**.

Messages use **keyed records** (hashed host authority for consistent partitioning) and **JSON payloads**.

---

## Topics

### `fc.tocrawl` / `uris.tocrawl`

| Property | Value |
|----------|-------|
| Direction | **Consumer** (input to crawler) |
| Environment variable | `KAFKA_TOCRAWL_TOPIC` |
| Default name | `uris.tocrawl` |
| Docker name | `fc.tocrawl` |

URLs queued for crawling. The Heritrix crawler reads from this topic via `KafkaUrlReceiver` and schedules each URL for fetch. External systems (e.g. crawl scheduling services) write seed URLs here; the scoper also writes back URLs it has determined are in scope.

**Key files:**
- `ukwa-heritrix/src/main/java/uk/bl/wap/crawler/frontier/KafkaUrlReceiver.java` (line 227)
- `jobs/frequent/crawler-beans.cxml` (line 54)
- `jobs/beans/scoper/scope.xml` (line 232)
- `docker-compose.yml` (line 28)

---

### `fc.candidates` / `uris.candidates`

| Property | Value |
|----------|-------|
| Direction | **Consumer** (input to scoper) |
| Environment variable | `KAFKA_CANDIDATES_TOPIC` |
| Default name | `uris.candidates` |
| Docker name | `fc.candidates` |

URLs discovered by the crawler as outlinks, proposed for inclusion in the crawl. The scoper service (`KafkaStreamHandler`) reads from this topic, evaluates each URL against scope rules, and routes it to either `fc.tocrawl` (accepted) or `fc.discarded` (rejected).

**Key files:**
- `ukwa-heritrix/src/main/java/uk/bl/wap/scoper/KafkaStreamHandler.java` (line 171)
- `jobs/frequent/crawler-beans.cxml` (line 692)
- `jobs/beans/scoper/scope.xml` (line 214)
- `docker-compose.yml` (line 30)

---

### `fc.crawled` / `uris.crawled`

| Property | Value |
|----------|-------|
| Direction | **Producer** (output from crawler) |
| Environment variable | `KAFKA_CRAWLED_TOPIC` |
| Default name | `uris.crawled` |
| Docker name | `fc.crawled` |

Crawl log feed. Published by `KafkaKeyedCrawlLogFeed` (a post-processor) after every URL is fetched. Each message contains the crawl result record (HTTP status, content type, digest, timestamp, etc.) and serves as the authoritative record for downstream systems such as the CDX indexer and QA tools.

**Key files:**
- `ukwa-heritrix/src/main/java/uk/bl/wap/crawler/postprocessor/KafkaKeyedCrawlLogFeed.java` (line 125)
- `jobs/frequent/crawler-beans.cxml` (line 638)
- `docker-compose.yml` (lines 35, 150)

---

### `fc.inscope` / `uris.inscope`

| Property | Value |
|----------|-------|
| Direction | **Producer** (output from crawler candidate chain) |
| Environment variable | `KAFKA_INSCOPE_TOPIC` |
| Default name | `uris.inscope` |
| Docker name | `fc.inscope` |

A copy of every URL that passed scope checks. Published by `KafkaKeyedToCrawlFeed` in the candidate chain. Used for disaster recovery, auditing, and verifying that the scoper's decisions are consistent with what the crawler actually attempts to fetch.

**Key files:**
- `ukwa-heritrix/src/main/java/uk/bl/wap/crawler/postprocessor/KafkaKeyedToCrawlFeed.java`
- `jobs/frequent/crawler-beans.cxml` (line 262)
- `docker-compose.yml` (line 32)

---

### `fc.discarded` / `uris.discarded`

| Property | Value |
|----------|-------|
| Direction | **Producer** (output from scoper / crawler) |
| Environment variable | `KAFKA_DISCARDED_TOPIC` |
| Default name | `uris.discarded` |
| Docker name | `fc.discarded` |

URLs rejected by scope rules. Published by `KafkaKeyedDiscardedFeed` when a candidate URL falls outside the defined crawl scope. Useful for auditing scope configuration, debugging missed URLs, and understanding what the crawler chose not to follow.

**Key files:**
- `ukwa-heritrix/src/main/java/uk/bl/wap/crawler/postprocessor/KafkaKeyedDiscardedFeed.java`
- `jobs/frequent/crawler-beans.cxml` (line 65)
- `jobs/beans/scoper/scope.xml` (line 225)
- `docker-compose.yml` (line 34)

---

### `heritrix-crawl-log` *(secondary)*

| Property | Value |
|----------|-------|
| Direction | **Producer** (output from scoper service) |
| Environment variable | — |
| Notes | Alternative/secondary crawl log producer used within the scoper service |

An additional crawl log topic published by `KafkaStreamProducer` inside the scoper service. Less commonly referenced than `fc.crawled`; may be used for scoper-specific logging or legacy compatibility.

**Key files:**
- `ukwa-heritrix/src/main/java/uk/bl/wap/scoper/KafkaStreamProducer.java` (line 98)

---

## Pipeline Flow

```
External / Scheduler
        │
        ▼
  [fc.tocrawl] ◄──────────────────────────────────┐
        │                                          │
        ▼                                          │ (accepted)
  Heritrix Crawler                        [fc.inscope]
  (KafkaUrlReceiver)                             ▲
        │                                         │
        │ discovers outlinks                      │
        ▼                                         │
  [fc.candidates]                                 │
        │                                         │
        ▼                                         │
  Scoper Service                                  │
  (KafkaStreamHandler)                            │
        │                                         │
        ├── in scope ────────────────────────────►┘
        │                     also writes back to [fc.tocrawl]
        │
        └── out of scope ──► [fc.discarded]
        
  Heritrix Crawler (fetch complete)
        │
        └──► [fc.crawled]  (crawl log records)
        └──► [heritrix-crawl-log]  (scoper service log)
```

---

## Topic Configuration Reference

Defined in `docker-compose.yml`:

```
KAFKA_CREATE_TOPICS:
  fc.candidates:16:1  --config=compression.type=snappy
  fc.tocrawl:16:1     --config=compression.type=snappy
  fc.crawled:16:1     --config=compression.type=snappy
  fc.discarded:16:1   --config=compression.type=snappy
  fc.inscope:16:1     --config=compression.type=snappy
```

| Topic | Partitions | Replication | Compression |
|-------|-----------|-------------|-------------|
| `fc.candidates` | 16 | 1 | snappy |
| `fc.tocrawl` | 16 | 1 | snappy |
| `fc.crawled` | 16 | 1 | snappy |
| `fc.discarded` | 16 | 1 | snappy |
| `fc.inscope` | 16 | 1 | snappy |
