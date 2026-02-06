# Streaming Market Pipeline (Kafka + Airflow) — Learning Build

## Goal
Build a streaming ingestion pipeline with Kafka + a continuous consumer, and use Airflow only for batch orchestration (QC, backfills, daily aggregates).

## Inspiration / Credits
Inspired by the ideas in simardeep1792/Data-Engineering-Streaming-Project (Kafka + Spark + Airflow + Docker). This repository is my own implementation and structure.
Source reference: https://github.com/simardeep1792/Data-Engineering-Streaming-Project

## What will be different in my implementation
- My own folder structure + code (clean-room build)
- Better separation: streaming consumer vs Airflow orchestration
- Data quality checks and reproducible runs
- Clear docs: assumptions, failure modes, and recovery steps

## Quick Start (coming soon)
- docker compose up
- produce events
- consume to storage
- Airflow DAG aggregates daily dataset
