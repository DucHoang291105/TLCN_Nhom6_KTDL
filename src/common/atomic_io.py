import hashlib
import json
import os
import tempfile
from pathlib import Path


def file_sha256(path, normalize=False):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        if normalize:
            digest.update(handle.read().replace(b'\r\n', b'\n'))
        else:
            for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path, payload, mode=0o644):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
