"""Command-line interface for polytape.

This is the step-1 scaffolding stub so that ``python -m polytape`` and the
``polytape`` console script resolve and run. Full argument parsing, validation,
and orchestration are implemented in step 2.
"""

from __future__ import annotations

import sys

from polytape import __version__


def main(argv: list[str] | None = None) -> int:
    """Console entry point.

    Args:
        argv: Argument list excluding the program name. Defaults to
            ``sys.argv[1:]`` when ``None``.

    Returns:
        A process exit code (0 on success).
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("-V", "--version"):
        print(f"polytape {__version__}")
        return 0
    print(f"polytape {__version__} - CLI not yet implemented (scaffolding).")
    print("Argument parsing lands in step 2.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
