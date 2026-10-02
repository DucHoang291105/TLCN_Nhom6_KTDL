FROM apache/superset:6.0.0-dev

USER root
RUN /app/.venv/bin/pip install --no-cache-dir trino==0.336.0
USER superset
