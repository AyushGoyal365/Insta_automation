"""Minimal .env loader for local development.

In CI/production (e.g. GitHub Actions) real environment variables / secrets are
set directly, so .env won't exist there and this is a no-op.
"""

import os


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())
