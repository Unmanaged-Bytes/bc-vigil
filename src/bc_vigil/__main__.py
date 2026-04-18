from __future__ import annotations

import argparse

import uvicorn

from bc_vigil.config import settings


def main() -> None:
    parser = argparse.ArgumentParser(prog="bc-vigil")
    parser.add_argument("--host", default=settings.host)
    parser.add_argument("--port", type=int, default=settings.port)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    uvicorn.run(
        "bc_vigil.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
