from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import WikibaseIntegrator

load_dotenv()

import os

READ_CTFG_FROM_CACHE: bool = "TRUE".startswith(
    os.getenv("READ_CTFG_FROM_CACHE", "False").upper()
)


@dataclass
class airtable:
    api_key: str = os.environ["AIRTABLE_API_KEY"]
    base_id: str = os.environ["AIRTABLE_BASE_ID"]


wbi_config["USER_AGENT"] = (
    "AutomationDev/0.1 (https://www.wikidata.org/wiki/User:TECCLESTON-TECH"
)

wbi = WikibaseIntegrator()
