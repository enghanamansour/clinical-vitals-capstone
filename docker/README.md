# Infrastructure (Docker)

Local services the pipeline depends on. Airflow is added in Stage 7.

| Service   | Port | Purpose |
|-----------|------|---------|
| kafka     | 9092 | KRaft broker; host clients connect to `localhost:9092` |
| kafka-ui  | 8080 | web UI to browse topics and messages |
| qdrant    | 6333 | vector store for the RAG stage |

## Bring it up

```powershell
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps      # wait until kafka is "healthy"
```

## Create the topics

```powershell
python -m src.ingestion.admin
# -> creates vitals.raw (3 partitions) and vitals.deadletter (1 partition)
```

## Tear down

```powershell
docker compose -f docker/docker-compose.yml down          # keep qdrant data
docker compose -f docker/docker-compose.yml down -v       # also wipe qdrant data
```

## Notes

- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` - topics are created explicitly by
  `src/ingestion/admin.py` so partition counts are deterministic.
- Two listeners: `HOST` (`localhost:9092`, used from Windows) and `DOCKER`
  (`kafka:19092`, used by `kafka-ui`).
