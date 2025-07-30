from typing import Any, Optional
from util import *
from pyairtable import Api
import os
import pickle
from collections import defaultdict
from pprint import pprint


api_key = os.environ["AIRTABLE_API_KEY"]
api = Api(api_key)

base_id = os.environ["AIRTABLE_BASE_ID"]
base = api.base(base_id)

from pyairtable.orm import Model, fields as F


class WikidataProperty(Model):
    pid_num = F.IntegerField("PID")
    label = F.SingleLineTextField("Label")
    description = F.MultilineTextField("Description")
    statements = F.LinkField("Statements", "Wikidata Statements")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Properties"
        memoize = True


class WikidataItem(Model):
    qid = F.SingleLineTextField("QID")
    label = F.SingleLineTextField("Label")
    description = F.MultilineTextField("Description")
    statements = F.LinkField("Statements", "Wikidata Statements")
    listings = F.LinkField("Listings", "Listing")
    url = F.UrlField("Wikidata Page", readonly=True)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Items"
        memoize = True

    @staticmethod
    def from_wiki_match(m: dict, keep_unknowns: bool = False):
        keyMapping = {
            "qid": "id",
            "label": "label",
            "description": "description",
        }
        mappable = {k: m[v] for k, v in keyMapping.items()}
        return WikidataItem(**mappable)


class WikidataStatement(Model):
    qid_num = F.IntegerField("QID")
    label = F.SingleLineTextField("Label")
    description = F.MultilineTextField("Description")
    property = F.SingleLinkField("Wikidata Property", WikidataProperty)
    item = F.SingleLinkField("Wikidata Item", WikidataItem)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Statements"
        memoize = True


class Listing(Model):
    name = F.SingleLineTextField("Project name")
    wikidata_item = F.SingleLinkField("Wikidata Item", WikidataItem)
    wikidata_suggestions = F.LinkField("Wikidata Item Suggestions", WikidataItem)
    type = F.MultipleSelectField("Type")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Listings"
        memoize = True


listings = Listing.meta.table
log(listings)
log(listings.first())
log(listings.id)


def deploy_fields() -> None:
    log("Deploying missing fields (as necessary)")
    models: dict[Any, dict[Any, dict[str, Any]]] = {
        WikidataItem: {
            WikidataItem.qid: {
                "field_type": "singleLineText",
            },
            WikidataItem.listings: {
                "field_type": "multipleRecordLinks",
                "description": "CTFG listings that are suspected to match this wikidata item",
                "options": {
                    "linkedTableId": Listing.meta.table.id,
                    # "isReversed": True,
                    # "prefersSingleRecordLink": True,
                },
            },
            WikidataItem.url: {
                "field_type": "singleLineText",
            },
        },
        Listing: {
            Listing.wikidata_item: {
                "field_type": "multipleRecordLinks",
                "description": "The matching Wikidata item selected by CTFG (staff, or maybe airtable automation)",
                "options": {
                    "linkedTableId": WikidataItem.meta.table.id,
                    # "isReversed": True,
                    # "prefersSingleRecordLink": True,
                },
            },
            Listing.wikidata_suggestions: {
                "field_type": "multipleRecordLinks",
                "description": "Wikidata's suggested matches for the project name",
                "options": {
                    "linkedTableId": WikidataItem.meta.table.id,
                    # "isReversed": True,
                    # "prefersSingleRecordLink": False,
                },
            },
        },
    }
    for model, fields in models.items():
        log(f'Checking Model: "{model.meta.table.name}"')
        current_field_names: list[str] = [
            f.name for f in model.meta.table.schema().fields
        ]
        for field, params in fields.items():
            name: str = field.field_name
            if name not in current_field_names:
                log(f'attempting to deploy "{name}"')
                base.table(model.meta.table.id).create_field(
                    name,
                    **params,
                )
    log("Finished deploying fields")


def get_records(from_cache=True) -> list[Listing]:
    log("Getting airtable records...")
    cache_fp = "cache/ctfg.pickle"
    if from_cache:
        with open(cache_fp, "rb") as f:
            items: list[Listing] = pickle.load(f)
    else:
        items = Listing.all(memoize=True)

        log("Serializing results to pickle")
        with open(cache_fp, "wb") as f:
            pickle.dump(items, f)

    log(f"Found {len(items)} items")

    log(f"Example project name: {items[0].name}")

    return items


def summarize_types(items: list[Listing]):
    types: defaultdict[frozenset[str], int] = defaultdict(int)
    for x in items:
        types[frozenset(x.type)] += 1

    log("Item counts by type")
    for type, count in sorted(types.items(), key=lambda x: -x[1]):
        print(count, list(type))
    return types


from more_itertools import partition


def partition_matched(items: list[Listing]) -> tuple[list[Listing], list[Listing]]:
    unmatched, matched = partition(lambda x: x.wikidata_item, items)
    return (list(unmatched), list(matched))


from itertools import batched


def upsert_matches(wiki_matches: dict[Listing, list[dict[str, Any]]]):
    log("Updating CTFG with matching wikibase IDs...")
    with_wiki_items = {
        x: [WikidataItem.from_wiki_match(m) for m in matches]
        for x, matches in wiki_matches.items()
        if matches
    }
    wiki_items = list(set([x for y in with_wiki_items.values() for x in y]))

    log("Example item")
    pprint(wiki_items[0].to_record())
    WikidataItem.batch_save(wiki_items)

    for x, matches in with_wiki_items.items():
        x.wikidata_suggestions = matches
    Listing.batch_save(list(with_wiki_items.keys()))
