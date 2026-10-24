"""Nonblocking advisory lock, released by the OS if the writer crashes."""
import errno
import hashlib
import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def file_writer_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if path.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        acquired = False
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            yield
        except OSError as error:
            if not acquired and error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise RuntimeError(f"Another writer holds {path.name}; do not run manual jobs and Airflow concurrently") from error
            raise
        finally:
            if acquired:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle, fcntl.LOCK_UN)


def history_writer_lock(target):
    key = hashlib.sha256(target.encode()).hexdigest()[:24]
    return file_writer_lock(Path(__file__).resolve().parents[2] / "outputs/locks" / f"history_{key}.lock")
