from collections import defaultdict
from pprint import pprint
from typing import Any
from util import *
import pickle
from wikibaseintegrator.wbi_helpers import search_entities
from random import sample
import ctfg

from wikibaseintegrator.entities.property import PropertyEntity


def get_matches(
    items: list[ctfg.Listing],
    max_attempts=None,
    from_cache=False,
    max_results: int = 50,
) -> dict[ctfg.Listing, list[dict[str, Any]]]:
    matchable_items = [x for x in items if x.name]
    attempting_items = (
        sample(matchable_items, max_attempts) if max_attempts else matchable_items
    )
    log(
        f"Searching for wikibase matches for {len(attempting_items)} {'random' if max_attempts else 'matchable'} items..."
    )
    cache_fp = "cache/wiki_orgs.pickle"
    if from_cache:
        with open(cache_fp, "rb") as f:
            wiki_matches = pickle.load(f)
    else:
        wiki_matches = {}
        for x in attempting_items:
            raw_matches = search_entities(
                x.name, config.LANGUAGE_CODE, dict_result=True, max_results=max_results
            )
            wiki_matches[x] = raw_matches

        if not max_attempts:
            log("Serializing results to pickle")
            with open(cache_fp, "wb") as f:
                pickle.dump(wiki_matches, f)

    return wiki_matches


def summarize_matches(wiki_matches):
    count_of_counts = defaultdict(int)
    for x in wiki_matches.values():
        count_of_counts[len(x)] += 1

    wiki_total_matches = sum(k * v for k, v in count_of_counts.items())
    matched_item_count = sum(v for k, v in count_of_counts.items() if k != 0)
    log(f"{wiki_total_matches} Wiki Matches across {matched_item_count} items.")

    log("Wikibase match count histogram:")
    for bucket, count in sorted(count_of_counts.items()):
        print(bucket, count)

    log("Example match:")
    pprint([x for y in wiki_matches.values() for x in y][0])
    return count_of_counts


def get_jsons(matched_items: list[ctfg.Listing]):
    """Not really used any more (just for reporting urls found)"""

    from random import sample

    log("Getting wikidata json for confirmed matches...")
    matched_wikis = {
        x: config.wbi.item.get(x.wikidata_item.qid).get_json()
        for x in sample(matched_items, min(50, len(matched_items)))
        if x.wikidata_item
    }
    log("Example wikidata json:")
    pprint(list(matched_wikis.values())[0])
    return matched_wikis


def get_urls(matched_wikis):
    """Not really used any more (just for reporting urls found)"""
    urls = {
        k: v["claims"]["P856"]
        for k, v in matched_wikis.items()
        if "P856" in v["claims"]
    }
    log(f"Urls found for {len(urls)} items.")
    return urls
