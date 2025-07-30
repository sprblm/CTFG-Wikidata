from dotenv import load_dotenv

load_dotenv()

import os

READ_CTFG_FROM_CACHE: bool = "true".startswith(
    os.getenv("READ_CTFG_FROM_CACHE", "False").lower()
)
