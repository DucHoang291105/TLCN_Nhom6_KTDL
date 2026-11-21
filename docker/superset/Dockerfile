FROM apache/superset@sha256:100af35c5a3c96384d4092ae4bd7fffb8c23d361e9a5a8b94312c50426aa1144

USER root
RUN /app/.venv/bin/pip install --no-cache-dir trino==0.336.0
USER superset
