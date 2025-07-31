from datetime import datetime
import config


def log(message):
    print(f"\n{datetime.now().isoformat()} {message}", flush=True)
