"""Compatibility entry point; use python -m src.quality.release_check."""
from src.quality.release_check import *  # noqa: F403
from src.quality.release_check import main

if __name__ == "__main__":
    main()
