from util import *
from pyairtable import Api
import os
import pickle
from collections import defaultdict
from pprint import pprint


api = Api(os.environ["AIRTABLE_API_KEY"])
bases = {
    "timCopy": "appYLSUaPLATuDnYV",
    "dev": "appWfbJkmPgyX0nM4",
    "snapshot": "appFHP1OYJg69gvpK",
}

base = api.table(bases["dev"], "Listings")


def get_records(from_cache=True):
    log("Getting airtable records...")
    cache_fp = "cache/ctfg.pickle"
    if from_cache:
        with open(cache_fp, "rb") as f:
            items = pickle.load(f)
    else:
        items = [x for x in base.all()]

        log("Serializing results to pickle")
        with open(cache_fp, "wb") as f:
            pickle.dump(items, f)

    log(f"Found {len(items)} items")

    log(f"Example project name: {items[0]['fields']['Project name']}")

    return items


def summarize_types(items):
    types = defaultdict(int)
    for x in items:
        types[frozenset(x["fields"].get("Type", []))] += 1
    log("Item counts by type")

    for x in sorted(types.items(), key=lambda x: -x[1]):
        print(x[1], list(x[0]))
    return types


def partition_matched(items):
    matched_items = [x for x in items if "Wikidata ID (Number)" in x["fields"]]
    unmatched_items = [x for x in items if "Wikidata ID (Number)" not in x["fields"]]
    return (matched_items, unmatched_items)


def upsert_matches(wiki_matches):
    log("Updating CTFG with matching wikibase IDs...")
    updates = [
        {"id": key, "fields": {"Wikidata ID suggestions": "\n".join(matches)}}
        for (key, matches) in wiki_matches.items()
    ]
    base.batch_update(updates)
    return updates


def update_urls(urls):

    updates = [
        {
            "id": k,
            "fields": {
                "Wikidata Official Page Suggestions": "\n".join(
                    [x["mainsnak"]["datavalue"]["value"] for x in v]
                )
            },
        }
        for k, v in urls.items()
    ]
    pprint(updates)

    log("Updating URLs in matched items")
    base.batch_update(updates)
    return updates
