# Streaming Market Pipeline (Kafka + Airflow) 

## Goal
Build a streaming ingestion pipeline with Kafka + a continuous consumer, and use Airflow only for batch orchestration (QC, backfills, daily aggregates).

## Quick Start 
- docker compose up
- produce events
- consume to storage
- Airflow DAG aggregates daily dataset
