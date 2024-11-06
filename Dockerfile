FROM python:3.10-slim
LABEL authors="Space Dimension"

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/app

RUN mkdir -p /app/data

VOLUME ["/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]