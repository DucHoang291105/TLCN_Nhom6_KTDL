import hashlib

import pytest

from src.common.atomic_io import file_sha256
from src.orchestration.maintenance import sha256
from src.orchestration.pipeline import csv_checksum
from src.orchestration.publication import checksum


@pytest.mark.parametrize(
    "payload",
    [b"", b"a\r\nb\nc\rd", b"x" * (1024 * 1024 - 1) + "\r\nNhà Việt".encode(), bytes(range(256)) * 8193],
    ids=["empty", "mixed-newlines", "block-boundary", "binary"],
)
def test_checksums_preserve_binary_and_normalized_contracts(tmp_path, payload):
    path = tmp_path / "input.bin"
    path.write_bytes(payload)
    raw = hashlib.sha256(payload).hexdigest()
    normalized = hashlib.sha256(payload.replace(b"\r\n", b"\n")).hexdigest()
    for function in (file_sha256, sha256, csv_checksum, checksum):
        assert function(path) == function(str(path)) == raw
    assert file_sha256(path, True) == checksum(path, normalize=True) == normalized
    assert path.read_bytes() == payload


def test_missing_checksum_input_does_not_create_files(tmp_path):
    path = tmp_path / "missing.bin"
    for function in (file_sha256, sha256, csv_checksum, checksum):
        with pytest.raises(FileNotFoundError):
            function(path)
    assert list(tmp_path.iterdir()) == []
