import ast
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.bronze.historical_lineage import validate_pinned


@pytest.mark.parametrize('field', ['sha256', 'size_bytes', 'rows', 'schema'])
def test_historical_immutable_content_is_not_just_row_count(field):
    current = {'sha256': 'a', 'size_bytes': 123, 'rows': 10, 'schema': ['title']}
    old = dict(current)
    old[field] = 'changed'
    with pytest.raises(ValueError, match='Immutable historical input changed'):
        validate_pinned('mogi', current, {'sources': {'mogi': old}})


def test_bronze_tools_import_without_credentials_or_pyspark(monkeypatch):
    monkeypatch.delenv('MINIO_ROOT_USER', raising=False)
    monkeypatch.delenv('MINIO_ROOT_PASSWORD', raising=False)
    compare = importlib.import_module('src.bronze.compare_snapshot_batches')
    importlib.import_module('src.bronze.ingest_bronze')
    assert compare.pct(1, 4) == 25.0
    assert compare.pct(1, 0) == 0.0
    with pytest.raises(ValueError):
        compare.validate_batch_id('20260230')


def test_location_mismatch_never_drops_the_existing_table():
    # Execute the production function without requiring a local Spark install.
    path = Path(__file__).resolve().parents[2] / 'src/common/spark_session.py'
    node = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == 'write_iceberg_table')
    spark = SimpleNamespace(sql=Mock())
    frame = SimpleNamespace(sparkSession=spark, writeTo=Mock())
    namespace = {'DataFrame': object, 'table_location': lambda _: 's3://new/table',
                 '_current_location': lambda *a: 's3://old/table'}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    with pytest.raises(RuntimeError, match='no table was dropped'):
        namespace['write_iceberg_table'](frame, 'lakehouse.gold.fact_listing')
    spark.sql.assert_not_called()
    frame.writeTo.assert_not_called()
