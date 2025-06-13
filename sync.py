#!/usr/bin/env python
# coding: utf-8
from dotenv import load_dotenv

load_dotenv()

from pyairtable import Api
import os

api = Api(os.environ['AIRTABLE_API_KEY'])
bases = {'timCopy': 'appYLSUaPLATuDnYV'}

from datetime import datetime
def log(message):
  print(f'\n{datetime.now().isoformat()} {message}', flush=True)

import pickle

ctfg = api.table(bases['timCopy'], 'Listings')

def get_ctfg(from_cache=True):
  log('Getting airtable records...')
  cache_fp = 'cache/ctfg.pickle'
  if from_cache:
    with open(cache_fp, 'rb') as f:
      items = pickle.load(f)
  else:
    items = [x for x in ctfg.all() if 'Organization' in x['fields'].get('Type', [])]

    log('Serializing results to pickle')
    with open(cache_fp, 'wb') as f:
      pickle.dump(items, f)

  log(f'Found {len(items)} items')
  return items

items = get_ctfg()

log(f'Example project name: {items[0]['fields']['Project name']}')

from collections import defaultdict

types = defaultdict(int)
for x in items:
    types[frozenset(x['fields'].get('Type', []))] += 1
log('Item counts by type')

for x in sorted(types.items(), key=lambda x: -x[1]):
    print(x[1], list(x[0]))

from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config

wbi_config['USER_AGENT'] = 'AutomationDev/0.1 (https://www.wikidata.org/wiki/User:TECCLESTON-TECH'

wbi = WikibaseIntegrator()

from wikibaseintegrator.wbi_helpers import search_entities

from random import sample
def get_wiki_matches(items, max_attempts=None, from_cache=False):
  matchable_items = [x for x in items if 'Project name' in x['fields']]
  attempting_items = sample(matchable_items, max_attempts) if max_attempts else matchable_items
  log(f'Searching for wikibase matches for {len(attempting_items)} {'random' if max_attempts else 'matchable'} items...')
  cache_fp = 'cache/wiki_orgs.pickle'
  if from_cache:
    with open(cache_fp, 'rb') as f:
      wiki_matches = pickle.load(f)
  else:
    wiki_matches = {}
    for x in attempting_items:
      wiki_matches[x['id']] = search_entities(x['fields']['Project name'])

    if not max_attempts:
      log('Serializing results to pickle')
      with open(cache_fp, 'wb') as f:
        pickle.dump(wiki_matches, f)

  return wiki_matches

matched_items = [x for x in items if 'Wikidata ID (Number)' in x['fields']]
unmatched_items = [x for x in items if 'Wikidata ID (Number)' not in x['fields']]

log('Getting wikidata json for confirmed matches...')
matched_wikis = {x['id']: wbi.item.get('Q' + str(x['fields']['Wikidata ID (Number)'])).get_json() for x in sample(matched_items, 50)}
urls = {k: v['claims']['P856'] for k, v in matched_wikis.items() if 'P856' in v['claims']}
from pprint import pprint
updates = [{'id': k, 'fields': {'Wikidata Official Page Suggestions': '\n'.join([x['mainsnak']['datavalue']['value'] for x in v])}} for k, v in urls.items()]
pprint(updates)
log(f'Urls found for {len(urls)} items.')

log('Updating URLs in matched items')
ctfg.batch_update(updates)
wiki_matches = get_wiki_matches(unmatched_items, max_attempts=50)

count_of_counts = defaultdict(int)
for x in wiki_matches.values():
    count_of_counts[len(x)] += 1

wiki_total_matches = sum(k * v for k, v in count_of_counts.items())
matched_item_count = sum(v for k, v in count_of_counts.items() if k != 0)
log(f'{wiki_total_matches} Wiki Matches across {matched_item_count} items.')

log('Wikibase match count histogram:')
for bucket, count in sorted(count_of_counts.items()):
  print(bucket, count)


update_matches_message = 'Updating CTFG with matching wikibase IDs...'
if True:
  log(update_matches_message)
  ctfg.batch_update([{'id': key, 'fields': {'Wikidata ID suggestions': '\n'.join(matches)}} for (key, matches) in wiki_matches.items()])
else:
   log(f'Skipping: {update_matches_message}')

