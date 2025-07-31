# CTFG-Wikidata

This project routinely and automatically cross-pollinates data between Wikidata and Civic Tech Field Guide.

Primarily, it should be seen as a two-way ELT or sync job,
but it can also (gradually) expand its own scope like a web crawler.

Not only does it put the two databases in conversation with each other,
it also puts them in conversation with humans.
Specifically, it will sometimes submit suggested changes for human review.
Once humans confirm or refine those suggestions
(for example confirming entity resolution),
it will likely lead to new points of integration
(e.g. syncing attributes of the entity between the two databases).

# Process
1. Extract: Pull CTFG records (of relevant `Type`s, like "Organization").
1. Extract: Search Wikidata for IDs to match any Orgs that don't have one yet.
1. Load: Update CTFG with (possibly multiple) matching Wikidata IDs.
1. Load: For well-matched Wikidata IDs, pull entire record into special field of CTFG.
1. Transform: Present suggested updates in new field(s) of CTFG DB.
1. Transform: Make confident updates to Wikibase.
1. Transform (Bonus): List lower confidence Wikidata edits in special field in CTFG.

# Design and Maintenance Considerations
1. Don't rush (let data converge slowly over time).
1. Keep business logic out of this low-level integration
(e.g. let CTFG manually or automatically accept suggestions _within_ their own DB).
1. Keep incoming and outgoing fields separate from each other and the fields of record (supporting the above).

# Implementation Details
CTFG DB Engine: Airtable

questions:
1. make sure Wikidata IDs don't duplicate in airtable?

# First-time deployment
The regular run will deploy some things (like fields),
but consult [./manual_deployments.md](./manual_deployments.md) to see what needs to be deployed manually.

