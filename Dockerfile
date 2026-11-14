# Образ сервиса, маршрутизатора и клиента нагрузки (один образ — разные команды в Helm).
# База закреплена по digest, зависимости — requirements.lock с хешами.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
RUN apt-get update && apt-get install -y --no-install-recommends tini=0.19.* curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /srv
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY app/ app/
COPY loadgen/ loadgen/
COPY models/ models/
RUN useradd -u 10001 -m sim && chown -R sim /srv
USER 10001
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
# tini — PID 1: пересылает SIGTERM uvicorn и позволяет убить процесс сервиса kill -9 в опыте A
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "app.server"]
