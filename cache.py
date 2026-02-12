import json
from pathlib import Path
from loguru import logger

CACHE_FILE = Path("cache.json")

if not CACHE_FILE.exists():
    CACHE_FILE.write_text("{}", encoding="utf-8")

# def load_cache() -> dict:
#     if CACHE_FILE.exists():
#         with open(CACHE_FILE, "r", encoding="utf-8") as f:
#             return json.load(f)
#     return {}


def load_cache():
    cache = {}

    if not CACHE_FILE.exists():
        return cache

    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()

            if not line or line == "{}":
                continue

            try:
                obj = json.loads(line)
                cache.update(obj)

            except json.JSONDecodeError as e:
                logger.warning(
                    f"Skipping invalid cache line {line_no}: {e}"
                )

    return cache

def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

# def append_cache(data: dict):
#     with open(CACHE_FILE, "a", encoding="utf-8") as f:
#         f.write(json.dumps(data) + "\n")


def append_cache(records):
    if not records:
        return

    with open(CACHE_FILE, "a", encoding="utf-8") as f:
        for r in records:
            if not r:
                continue
            f.write(json.dumps({r["bundle_id"]: r}, ensure_ascii=False) + "\n")


