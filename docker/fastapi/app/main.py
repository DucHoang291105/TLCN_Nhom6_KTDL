from __future__ import annotations

import os

from fastapi import FastAPI


app = FastAPI(
    title="TLCN Real Estate Lakehouse API",
    version="0.1.0",
    description="API gateway for Gold data and dashboard services.",
)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "tlcn-fastapi", "status": "running", "docs": "/docs"}


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "healthy",
        "postgres_configured": bool(os.getenv("POSTGRES_DSN")),
        "trino_url": os.getenv("TRINO_URL", "not-configured"),
    }
