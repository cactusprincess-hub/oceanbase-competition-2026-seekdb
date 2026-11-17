FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/minivectordb \
    VECTORDB_DATA_DIR=/data

WORKDIR /opt/minivectordb
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt
COPY minivectordb ./minivectordb

CMD ["python", "-m", "minivectordb.protocol"]
