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
if False:
  log('Testing fetch of wikibase "human" record as json:')
  my_first_wikidata_item = wbi.item.get(entity_id='Q5')

  # to check successful installation and retrieval of the data, you can print the json representation of the item
  print(my_first_wikidata_item.get_json())

from wikibaseintegrator import wbi_helpers

def get_wiki_matches(items, from_cache=True):
  log(f'Searching for wikibase matches for {len(items)} items...')
  cache_fp = 'cache/wiki_orgs.pickle'
  if from_cache:
    with open(cache_fp, 'rb') as f:
      wiki_matches = pickle.load(f)
  else:
    wiki_matches = {x['id']: wbi_helpers.search_entities(x['fields']['Project name']) for x in items}

    log('Serializing results to pickle')
    with open(cache_fp, 'wb') as f:
      pickle.dump(wiki_matches, f)
  return wiki_matches

wiki_matches = get_wiki_matches(items)

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
if False:
  log(update_matches_message)
  for (key, matches) in wiki_matches.items():
    ctfg.batch_update([{'id': key, 'fields': {'Wikidata ID suggestions': '\n'.join(matches)}}])
else:
   log(f'Skipping: {update_matches_message}')
