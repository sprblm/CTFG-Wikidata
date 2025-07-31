#!/usr/bin/env python
# coding: utf-8

import config
import ctfg
import wiki

# ctfg.deploy_fields() # not normally run
items = ctfg.get_records(config.READ_CTFG_FROM_CACHE)
types = ctfg.summarize_types(items)
(unmatched_items, matched_items) = ctfg.partition_matched(items)

wiki_matches = wiki.get_matches(unmatched_items, max_attempts=15)
wiki_match_histogram = wiki.summarize_matches(wiki_matches)

matched_wikis = wiki.get_jsons(matched_items)
urls = wiki.get_urls(matched_wikis)

match_updates = ctfg.upsert_matches(wiki_matches)
