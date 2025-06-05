# coding: utf-8
from dotenv import load_dotenv

load_dotenv()

from pyairtable import Api
import os

api = Api(os.environ['AIRTABLE_API_KEY'])
bases = {'timCopy': 'appYLSUaPLATuDnYV'}

projectRecords = api.table(bases['timCopy'], 'Listings')

projectRecords.schema()
projectRecords.first()

ctfg = api.table(bases['timCopy'], 'Listings').all()

orgs = [x for x in ctfg if 'Organization' in x['fields'].get('Type', [])]

len(orgs)
orgs[0]['fields']['Project name']

from wikibaseintegrator import WikibaseIntegrator

wbi = WikibaseIntegrator()
my_first_wikidata_item = wbi.item.get(entity_id='Q5')

# to check successful installation and retrieval of the data, you can print the json representation of the item
print(my_first_wikidata_item.get_json())

from wikibaseintegrator import wbi_helpers

from wikibaseintegrator.wbi_config import config as wbi_config

wbi_config['USER_AGENT'] = 'AutomationDev/0.1 (https://www.wikidata.org/wiki/User:TECCLESTON-TECH'

print(wbi.item.get(entity_id='Q131852782').get_json())
wiki_orgs = {x['id']: wbi_helpers.search_entities(x['fields']['Project name']) for x in orgs}

from collections import defaultdict

count_of_counts = defaultdict(int)
for x in wiki_orgs.values():
    count_of_counts[len(x)] += 1

sorted(count_of_counts.items())

# from itertools import *

sum(count_of_counts.values())

types = defaultdict(int)
for x in ctfg:
    types[frozenset(x['fields'].get('Type', []))] += 1
types

for x in sorted(types.items(), key=lambda x: -x[1]):
    print(x[1], list(x[0]))

import pickle
with open('wiki_orgs', 'wb') as f:
    pickle.dump(wiki_orgs, f)
