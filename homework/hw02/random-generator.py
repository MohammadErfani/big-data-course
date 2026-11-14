import json
import random
import string
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path("input")
BATCH_SIZE = 1000
INTERVAL_SECONDS = 10

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def random_word():
    length = random.randint(1, 3)
    return "".join(random.choices(string.ascii_lowercase, k=length))

def random_query():
    word_count = random.randint(1, 2)
    return " ".join(random_word() for _ in range(word_count))

def generate_batch():
    return [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": random_query(),
        }
        for _ in range(BATCH_SIZE)
    ]

def main():
    while True:
        batch = generate_batch()

        # Write outside the input directory first.
        file_id = uuid.uuid4().hex
        temp_file = Path(f".batch_{file_id}.tmp")
        final_file = OUTPUT_DIR / f"batch_{file_id}.json"

        with temp_file.open("w", encoding="utf-8") as f:
            for record in batch:
                f.write(json.dumps(record) + "\n")

        # Atomically make the completed file available to Spark.
        temp_file.replace(final_file)

        print(f"Generated {len(batch)} queries: {final_file}")
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    main()