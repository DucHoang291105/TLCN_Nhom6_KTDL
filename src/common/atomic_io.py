"""Atomic local JSON publication; a failed write never truncates prior evidence."""
import json
import os
import tempfile
from pathlib import Path


def write_json_atomic(path, payload, mode=0o644):
    """Publish non-secret validation artifacts readable across container UIDs.

    Never use the default public mode for credentials. A secret caller must
    explicitly request mode=0o600 and a private parent directory.
    """
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
