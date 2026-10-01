# Identity Resolution - Mal Digital Bank Customer 360

This repo contains the identity resolution logic that feeds the golden customer record
(`dim_customer`) in the Mal Customer 360 platform. It is a component of the wider
assessment (see the parent `mal-assessment/` folder for the full architecture, ERD and
CAR design).

The job runs as part of the Silver → Gold layer in Glue (PySpark). The SQL variant is
kept in sync so the same rules can be re-run in Redshift for ad-hoc audits and for the
manual review workflow that data stewards use.

## Problem

Customer identity at Mal is fragmented across three systems that were never designed to
talk to each other:

- **T24 (core banking)** - authoritative for KYC, Emirates ID, risk rating. Populated
  at account opening. Older records don't always carry a validated Emirates ID and the
  `name_arabic` field is inconsistent in how diacritics and hamza are encoded.
- **Salesforce FSC (CRM)** - service and relationship data. Email and phone are the
  main handles. Contact records can predate the T24 record (lead → converted customer).
- **Digital Channels (Amplitude)** - mobile app analytics keyed by device fingerprint
  (`user_id`). Email arrives only after the user logs in, so a large chunk of events
  are pre-login and have to be stitched later.

Without resolution, the same customer shows up as three (sometimes more) rows across
the Silver layer and downstream CAR features double-count them. The job here assigns a
single `mal_customer_id` per real person and maintains a crosswalk so we can trace any
gold record back to its sources.

## Approach

Two passes. The deterministic pass runs first and covers most volume cheaply. The
probabilistic pass only sees what the first pass couldn't match, which keeps the fuzzy
join small enough to run in-memory on the review shard.

### Pass 1 - Deterministic

| Rule | Keys                                          | Confidence |
|------|-----------------------------------------------|------------|
| D1   | Emirates ID exact match (validated checksum)  | 1.00       |
| D2   | Phone (E.164 +971) + DOB                      | 0.95       |
| D3   | Email (lowercased, trimmed) + DOB             | 0.90       |

D1 only fires when Emirates ID is present and passes the mod-11 checksum. If T24 has
an unvalidated Emirates ID (happens on ~4% of pre-2019 records in the sample data), we
skip it and fall through to D2/D3.

### Pass 2 - Probabilistic

Only records that didn't match in Pass 1. Score is a weighted sum:

- Jaro-Winkler on normalized name - weight **0.40**
- DOB fuzzy year match (tolerance ±180 days) - weight **0.30**
- Phone similarity (last-6-digits + edit distance) - weight **0.20**
- Email domain match - weight **0.10**

Thresholds:

- **≥ 0.70** - auto-link, `match_method = 'probabilistic_auto'`
- **0.50 – 0.69** - write to `identity_review_queue`, no gold row until a steward
  resolves it
- **< 0.50** - new `mal_customer_id`, `match_method = 'new_customer'`

The 0.70 cutoff was tuned against a stewarded sample of 2,000 pairs. Anything below
this on names alone tends to catch father/son pairs sharing a phone.

### Survivorship (Gold layer)

Once records are linked, we pick which attribute wins:

| Attribute            | Winner                                          |
|----------------------|-------------------------------------------------|
| KYC status, risk_rating | T24 (regulatory system of record)            |
| Emirates ID          | T24                                             |
| Email, phone         | Most-recent verified (`verified_at` timestamp)  |
| Segment              | Highest tier: private > priority > affluent > mass |
| First seen date      | Earliest `created_at` across all sources        |
| Name (English)       | T24 if present, else Salesforce, else Amplitude |
| Name (Arabic)        | T24 only                                        |

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run the tests
pytest tests/ -v

# Run against the sample fixture
python -m python.identity_resolver --input fixtures/sample_data.json --output /tmp/resolved.json
```

The Glue job wrapper isn't in this repo - it lives in `mal-etl/glue-jobs/` in the main
platform repo and imports `IdentityResolver` from here as a wheel.

## Folder structure

```
identity-resolution-repo/
├── README.md
├── requirements.txt
├── sql/
│   └── identity_resolution.sql       # Redshift/Athena implementation
├── python/
│   ├── identity_resolver.py          # PySpark IdentityResolver class
│   └── utils.py                      # Normalization + similarity helpers
├── tests/
│   └── test_identity_resolver.py     # pytest suite
└── fixtures/
    └── sample_data.json              # Sample records with expected outcomes
```

## Known limitations / next steps

- **Arabic name matching is weak.** Jaro-Winkler on Unicode-normalized Arabic works
  for close variants but misses transliteration collisions (Mohammed / Mohamed /
  Muhammad). Next iteration should add a phonetic pass using a Buckwalter-style
  transliteration or a small Arabic-aware model.
- **No cross-batch stability guarantee.** `mal_customer_id` is assigned deterministically
  from a hash of the strongest key, but if a customer's Emirates ID appears for the
  first time in batch N+1, their prior ID (assigned from phone+DOB) becomes an alias
  rather than the primary. The crosswalk keeps history, but downstream jobs need to
  join through it, not through the raw ID.
- **Review queue has no SLA yet.** Assumption is 24h steward turnaround; needs to be
  formalized with Ops before Q2 launch.
- **No graph-based resolution.** Two-hop transitive matches (A↔B via phone, B↔C via
  email) are handled by re-running the deterministic pass over the newly-linked set,
  which converges in 2–3 iterations on the sample data but has no theoretical bound.
  A proper connected-components step (GraphFrames or Splink) is on the backlog for
  when volume crosses ~1M customers.
- **Consent isn't enforced here.** The resolver produces the linkage regardless of
  marketing consent. Downstream CAR population reads `consent_flags` and masks
  accordingly. Keeping identity separate from consent is deliberate - regulatory
  reporting needs to see everyone.
