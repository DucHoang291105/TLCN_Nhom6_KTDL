"""Keep Windows awake for one foreground command; restore the thread policy on exit."""
from __future__ import annotations
import argparse
import contextlib
import os
import subprocess


@contextlib.contextmanager
def awake():
    if os.name != 'nt':
        yield
        return
    import ctypes
    continuous, system_required = 0x80000000, 0x00000001
    set_state = ctypes.windll.kernel32.SetThreadExecutionState
    set_state.argtypes = [ctypes.c_uint]
    set_state.restype = ctypes.c_uint
    previous = set_state(continuous | system_required)
    if not previous:
        raise OSError('Cannot prevent idle sleep for this command')
    try:
        yield
    finally:
        set_state(previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    command = parser.parse_args().command
    if command[:1] == ['--']:
        command = command[1:]
    if not command:
        parser.error('A foreground command is required after --')
    with awake():
        # The child exit code is the result; nonzero is deliberately propagated.
        raise SystemExit(subprocess.run(command, check=False).returncode)


if __name__ == '__main__':
    main()
