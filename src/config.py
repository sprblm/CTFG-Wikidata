from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login
from wikibaseintegrator import WikibaseIntegrator

load_dotenv()

import os

READ_CTFG_FROM_CACHE: bool = "TRUE".startswith(
    os.getenv("READ_CTFG_FROM_CACHE", "False").upper()
)

POST_DETAILS_TO_CTFG: bool = "TRUE".startswith(
    os.getenv("POST_DETAILS_TO_CTFG", "False").upper()
)


@dataclass
class airtable:
    api_key: str = os.environ["AIRTABLE_API_KEY"]
    base_id: str = os.environ["AIRTABLE_BASE_ID"]


WIKIDATA_BOT_USERNAME = os.getenv("WIKIDATA_BOT_USERNAME", None)
WIKIDATA_BOT_PW = os.getenv("WIKIDATA_BOT_PW", None)


wbi_config["USER_AGENT"] = (
    "CTFG-Wikidata/0.1 (https://www.wikidata.org/wiki/User:TECCLESTON-TECH, https://github.com/sprblm/CTFG-Wikidata)"
)

if WIKIDATA_BOT_USERNAME and WIKIDATA_BOT_PW:
    wbi_login.Login(user=WIKIDATA_BOT_USERNAME, password=WIKIDATA_BOT_PW)

wbi = WikibaseIntegrator()

WIKIDATA_MAX_LISTINGS_TO_SEARCH: int = int(
    os.getenv("WIKIDATA_MAX_LISTINGS_TO_SEARCH", 5)
)

WIKIDATA_MAX_RESULTS_PER_SEARCH: int = int(
    os.getenv("WIKIDATA_MAX_RESULTS_PER_SEARCH", 5)
)


LANGUAGE_CODE = "en"
