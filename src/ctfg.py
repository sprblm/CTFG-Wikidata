from typing import Optional
from util import *
from pyairtable import Api
import os
import pickle
from collections import defaultdict
from pprint import pprint

api_key = os.environ["AIRTABLE_API_KEY"]
api = Api(api_key)
bases = {
    "timCopy": "appYLSUaPLATuDnYV",
    "dev": "appWfbJkmPgyX0nM4",
    "snapshot": "appFHP1OYJg69gvpK",
}
base_id = bases["dev"]

from pyairtable.orm import Model, fields as F


class WikidataProperty(Model):
    pid_num: int = F.IntegerField("PID")
    label: str = F.SingleLineTextField("Label")
    description: str = F.MultilineTextField("Description")
    statements = F.LinkField("Statements", "WikidataStatement")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Properties"


class WikidataItem(Model):
    qid_num: int = F.IntegerField("QID")
    label: str = F.SingleLineTextField("Label")
    description: str = F.MultilineTextField("Description")
    statements = F.LinkField("Statements", "WikidataStatement")
    listings = F.LinkField("Listings", "Listing")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Items"


class WikidataStatement(Model):
    qid_num: int = F.IntegerField("QID")
    label: str = F.SingleLineTextField("Label")
    description: str = F.MultilineTextField("Description")
    property = F.LinkField("Wikidata Property", WikidataProperty)
    item = F.LinkField("Wikidata Item", WikidataItem)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Statements"


class Listing(Model):
    name: Optional[str] = F.SingleLineTextField("Project name")
    wikidata_suggestions = F.LinkField("Wikidata Item Suggestions", WikidataItem)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Listings"
        id = "tblELFP9tGX07UZDo"


table = Listing.meta.table


def deploy_fields():
    log("Deploying missing fields (as necessary)")
    fields = {
        WikidataItem.listings.field_name: {
            "field_type": "multipleRecordLinks",
            "description": "CTFG listings that are suspected to match this wikidata item",
            "options": {
                "linkedTableId": Listing.meta.table.id,
                # "isReversed": True,
                # "prefersSingleRecordLink": True,
            },
        },
    }
    wikiItemsTable = WikidataItem.meta.table
    for name, params in fields.items():
        if name not in [field.name for field in wikiItemsTable.schema().fields]:
            wikiItemsTable.create_field(name, field_type=params['field_type'], description=params['description'], options=params['options'])


def get_records(from_cache=True):
    log("Getting airtable records...")
    cache_fp = "cache/ctfg.pickle"
    if from_cache:
        with open(cache_fp, "rb") as f:
            items = pickle.load(f)
    else:
        items = [x for x in table.all()]

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
    table.batch_update(updates)
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
    table.batch_update(updates)
    return updates
