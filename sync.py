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

def get_ctfg(from_cache=True):
  log('Getting airtable records...')
  cache_fp = 'cache/ctfg.pickle'
  if from_cache:
    with open(cache_fp, 'rb') as f:
      items = pickle.load(f)
  else:
    ctfg = api.table(bases['timCopy'], 'Listings').all()
    items = [x for x in ctfg if 'Organization' in x['fields'].get('Type', [])]

    log('Serializing results to pickle')
    with open(cache_fp, 'wb') as f:
      pickle.dump(items, f)

  print(f'\nFound {len(items)} items')
  return items

items = get_ctfg()

print(f'\nExample project name: {items[0]['fields']['Project name']}')

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
log('Testing fetch of wikibase "human" record as json:')
my_first_wikidata_item = wbi.item.get(entity_id='Q5')

# to check successful installation and retrieval of the data, you can print the json representation of the item
print(my_first_wikidata_item.get_json())

from wikibaseintegrator import wbi_helpers

def get_wiki_matches(items, from_cache=True):
  log('Searching for wikibase matches...')
  cache_fp = 'cache/wiki_orgs.pickle'
  if from_cache:
    with open(cache_fp, 'rb') as f:
      wiki_matches = pickle.load(f)
  else:
    wiki_matches = {x['id']: wbi_helpers.search_entities(x['fields']['Project name']) for x in items}

    log('Serializing results to pickle')
    with open(cache_fp, 'wb') as f:
      pickle.dump(wiki_orgs, f)
  return wiki_matches

wiki_orgs = get_wiki_matches(orgs)

count_of_counts = defaultdict(int)
for x in wiki_orgs.values():
    count_of_counts[len(x)] += 1

log('Wikibase matches per CTFG item:')
sorted(count_of_counts.items())

log('Wikibase match count histogram:')
sum(count_of_counts.values())


