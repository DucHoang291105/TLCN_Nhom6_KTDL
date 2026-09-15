# Lakehouse Architecture

Dự kiến:

Data Sources
-> Ingestion
-> Bronze (MinIO + Iceberg)
-> Silver Core / Feature / Location / History
-> Gold Market / Benchmark / Repricing / Data Quality
-> PostgreSQL + PostGIS
-> Python Dashboard
