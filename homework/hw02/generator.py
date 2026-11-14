
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path("input")
BATCH_SIZE = 100
INTERVAL_SECONDS = 5

WORDS = [
    "cat", "dog", "car", "bus", "sun",
    "sky", "red", "big", "hot", "ice",
]

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_query():
    count = random.randint(1, 2)
    words = random.choices(WORDS, k=count)
    return " ".join(words)


def generate_batch():
    return [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": generate_query(),
        }
        for _ in range(BATCH_SIZE)
    ]


def main():
    while True:
        batch = generate_batch()

        file_id = uuid.uuid4().hex
        temp_file = Path(f".batch_{file_id}.tmp")
        final_file = OUTPUT_DIR / f"batch_{file_id}.json"

        with temp_file.open("w", encoding="utf-8") as f:
            for record in batch:
                f.write(json.dumps(record) + "\n")

        temp_file.replace(final_file)

        print(f"Generated {len(batch)} queries: {final_file}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
