# Built with: docker compose up (context: project root, per docker-compose.yml)
# Only installs what api/main.py + db/connection.py actually need - not the
# full requirements.txt, which also includes pyspark/confluent-kafka for the
# Spark job and event producer. Those have no reason to be in this image.

FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.141.1 \
    "uvicorn[standard]==0.52.1" \
    sqlalchemy==2.0.30 \
    psycopg2-binary==2.9.9

# api/main.py does `from db.connection import get_engine` with a sys.path
# append for the parent directory - so db/ needs to be a sibling of api/
# inside the image too, matching the local dev layout.
COPY api/ ./api/
COPY db/ ./db/

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
