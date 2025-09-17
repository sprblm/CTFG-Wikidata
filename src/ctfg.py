from json import dumps
from typing import Any
from util import *
from pyairtable import Api
import os
import pickle
from collections import defaultdict
from pprint import pprint
from functools import lru_cache
from collections import defaultdict

api_key = config.airtable.api_key
base_id = config.airtable.base_id

api = Api(api_key)
base = api.base(base_id)

from pyairtable.orm import Model, fields as F


class WikidataProperty(Model):
    pid = F.SingleLineTextField("PID")
    label = F.SingleLineTextField("Label")
    description = F.MultilineTextField("Description")
    # statements = F.LinkField("Statements", "Wikidata Statements")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Properties"
        memoize = True

    @lru_cache(maxsize=None)
    @staticmethod
    def from_wikidata_id(pid: str):
        p = config.wbi.property.get(pid)
        converted = WikidataProperty(
            pid=p.id,
            label=str(p.labels.values.get(config.LANGUAGE_CODE)),
            description=str(p.descriptions.get(config.LANGUAGE_CODE)),
        )
        converted.save()
        return converted


class WikidataStatementValueAttribute(Model):
    uuid = F.SingleLineTextField("Identifier")
    key = F.SingleLineTextField("Value Attribute Key")
    value = F.SingleLineTextField("Value Attribute Value")
    # statement = F.LinkField("Statement", "Wikidata Statements")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Statement Value Attributes"


class WikidataStatementValue(Model):
    uuid = F.SingleLineTextField("Identifier")
    type = F.SingleLineTextField("Wikidata Type")
    json = F.MultilineTextField("Wikidata Value JSON")
    attributes = F.LinkField("Attributes", WikidataStatementValueAttribute)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Statement Value"

    @staticmethod
    def parse_value_attributes(
        value: dict | str, base_id: str
    ) -> list[WikidataStatementValueAttribute]:
        if isinstance(value, str):
            value = {"string": value}

        attributes = [
            WikidataStatementValueAttribute(
                uuid=base_id + str(k), key=str(k), value=str(v)
            )
            for k, v in value.items()
        ]
        WikidataStatementValueAttribute.batch_save(attributes)
        return attributes

    @staticmethod
    def from_wiki_dict(uuid: str, datavalue: dict):
        result = WikidataStatementValue(
            uuid=uuid,
            type=datavalue["type"],
            json=dumps(datavalue["value"], indent=2),
            attributes=WikidataStatementValue.parse_value_attributes(
                datavalue["value"], uuid
            ),
        )
        result.save()
        return result


class WikidataStatement(Model):
    uuid = F.SingleLineTextField("Identifier")
    property = F.SingleLinkField("Wikidata Property", WikidataProperty)
    datatype = F.SingleLineTextField("Data Type")
    value = F.SingleLinkField("Value", WikidataStatementValue)
    # item = F.SingleLinkField("Wikidata Item", WikidataItem)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Statements"

    @staticmethod
    def from_wiki_statement(statement: dict):
        uuid: str = statement["id"]

        # Ignore alternatives, qualifiers, and references
        statement = statement["mainsnak"]

        property = WikidataProperty.from_wikidata_id(statement["property"])
        datavalue = statement.get("datavalue", None)
        value = (
            WikidataStatementValue.from_wiki_dict(uuid, datavalue)
            if datavalue and config.POST_DETAILS_TO_CTFG
            else None
        )

        result = WikidataStatement(
            uuid=uuid, property=property, datatype=statement["datatype"], value=value
        )
        result.save()
        return result


class WikidataItem(Model):
    qid = F.SingleLineTextField("QID")
    label = F.SingleLineTextField("Label")
    description = F.MultilineTextField("Description")
    statements = F.LinkField("Statements", WikidataStatement)
    # listings = F.LinkField("Listing Suggestions", "Listing")
    # listing = F.LinkField("Listing Official", "Listing")
    url = F.UrlField("Wikidata Page", readonly=True)

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Wikidata Items"

    @staticmethod
    def from_wiki_match(m: dict, keep_unknowns: bool = False):

        keyMapping = {
            "qid": "id",
            "label": "label",
            "description": "description",
        }
        mappable = {k: m[v] for k, v in keyMapping.items()}

        claims = config.wbi.item.get(mappable["qid"]).claims.get_json()
        # pprint(claims)

        statements = [
            WikidataStatement.from_wiki_statement(s) for p in claims.values() for s in p
        ]
        result = WikidataItem(**mappable, statements=statements)
        result.save()
        return result


class Listing(Model):
    name = F.SingleLineTextField("Project name")
    wikidata_item = F.SingleLinkField("Wikidata Item Official", WikidataItem)
    wikidata_suggestions = F.LinkField("Wikidata Item Suggestions", WikidataItem)
    type = F.MultipleSelectField("Type")

    class Meta:
        api_key = api_key
        base_id = base_id
        table_name = "Listings"


def deploy_fields() -> None:
    log("Deploying missing fields (as necessary)")
    models: dict[Any, dict[Any, dict[str, Any]]] = {
        WikidataItem: {
            WikidataItem.qid: {
                "field_type": "singleLineText",
            },
            WikidataItem.label: {
                "field_type": "singleLineText",
            },
            WikidataItem.description: {
                "field_type": "singleLineText",
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


def get_records(from_cache=False) -> list[Listing]:
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


def upsert_matches(wiki_matches: dict[Listing, list[dict[str, Any]]]):
    with_wiki_items = {
        x: [WikidataItem.from_wiki_match(m) for m in matches]
        for x, matches in wiki_matches.items()
        if matches
    }
    wiki_items = list(set([x for y in with_wiki_items.values() for x in y]))
    log(f"Updating CTFG with {len(wiki_items)} matching wikibase IDs...")

    if wiki_items:
        log("Example item")
        pprint(wiki_items[0].to_record())

    for x, matches in with_wiki_items.items():
        x.wikidata_suggestions = matches
    Listing.batch_save(list(with_wiki_items.keys()))
