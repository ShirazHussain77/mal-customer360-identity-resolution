-- =============================================================================
-- identity_resolution.sql
-- -----------------------------------------------------------------------------
-- Mal Digital Bank — Customer 360
-- Silver-to-Gold identity resolution.
--
-- Inputs (Silver):
--   silver.t24_customer          — core banking, authoritative for KYC/EID
--   silver.sfdc_contact          — Salesforce FSC contacts
--   silver.amplitude_user        — Digital Channels (mobile) user profiles
--
-- Outputs (Gold):
--   gold.identity_crosswalk      — one row per (source_system, source_id, mal_customer_id)
--   gold.identity_review_queue   — probabilistic matches 0.50–0.69 for steward review
--   gold.dim_customer            — golden customer record after survivorship
--
-- Dialect: Redshift (also runs on Athena with minor changes noted inline).
-- Runs nightly after the Silver layer lands (~02:30 GST).
--
-- Confidence thresholds (see README):
--   Deterministic:  D1=1.00, D2=0.95, D3=0.90
--   Probabilistic:  >=0.70  auto-link
--                    0.50–0.69  manual review
--                    < 0.50  new mal_customer_id
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Step 0: Session settings. Keep the run isolated; we rebuild gold each cycle.
-- -----------------------------------------------------------------------------
SET search_path TO gold, silver, public;

BEGIN;

-- -----------------------------------------------------------------------------
-- Step 1: Normalize source records into a common shape.
--   - Phone: strip non-digits, apply UAE country code, produce E.164.
--   - Email: lower + trim.
--   - Name:  lower, remove common punctuation, collapse whitespace.
--   Note: Redshift lacks a first-class E.164 normalizer; the CASE below covers
--         the four UAE mobile shapes we see in the sample data. Anything more
--         exotic (landlines, non-UAE numbers) is left as raw and won't match
--         on D2 — the probabilistic pass will pick it up if the signal is there.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tmp_norm_t24;
CREATE TEMP TABLE tmp_norm_t24 AS
SELECT
    't24'::VARCHAR(16)                                          AS source_system,
    customer_id::VARCHAR(64)                                    AS source_id,
    NULLIF(TRIM(emirates_id), '')                               AS emirates_id_raw,
    -- Emirates ID checksum validation (mod-11). Only trust the ID if it passes.
    CASE
        WHEN emirates_id ~ '^784-[0-9]{4}-[0-9]{7}-[0-9]$'
             AND fn_validate_eid_checksum(emirates_id)          -- UDF: see mal-etl/udfs/eid_checksum
            THEN REPLACE(emirates_id, '-', '')
        ELSE NULL
    END                                                         AS emirates_id,
    dob,
    LOWER(TRIM(name_english))                                   AS name_english,
    name_arabic,
    CASE
        WHEN mobile ~ '^\+971[0-9]{9}$'    THEN mobile
        WHEN mobile ~ '^00971[0-9]{9}$'    THEN '+971' || SUBSTRING(mobile, 6)
        WHEN mobile ~ '^0[0-9]{9}$'        THEN '+971' || SUBSTRING(mobile, 2)
        WHEN mobile ~ '^971[0-9]{9}$'      THEN '+' || mobile
        ELSE NULL
    END                                                         AS phone_e164,
    LOWER(TRIM(email))                                          AS email,
    risk_rating,
    segment,
    kyc_status,
    created_at,
    updated_at
FROM silver.t24_customer
WHERE record_status = 'ACTIVE';

DROP TABLE IF EXISTS tmp_norm_sfdc;
CREATE TEMP TABLE tmp_norm_sfdc AS
SELECT
    'sfdc'::VARCHAR(16)                                         AS source_system,
    contact_id::VARCHAR(64)                                     AS source_id,
    NULL::VARCHAR(15)                                           AS emirates_id,  -- SFDC doesn't hold EID
    dob,
    LOWER(TRIM(first_name || ' ' || last_name))                 AS name_english,
    NULL::VARCHAR(200)                                          AS name_arabic,
    CASE
        WHEN phone ~ '^\+971[0-9]{9}$'     THEN phone
        WHEN phone ~ '^00971[0-9]{9}$'     THEN '+971' || SUBSTRING(phone, 6)
        WHEN phone ~ '^0[0-9]{9}$'         THEN '+971' || SUBSTRING(phone, 2)
        WHEN phone ~ '^971[0-9]{9}$'       THEN '+' || phone
        ELSE NULL
    END                                                         AS phone_e164,
    LOWER(TRIM(email))                                          AS email,
    NULL                                                        AS risk_rating,
    NULL                                                        AS segment,
    NULL                                                        AS kyc_status,
    created_date                                                AS created_at,
    last_modified_date                                          AS updated_at
FROM silver.sfdc_contact
WHERE is_deleted = FALSE;

DROP TABLE IF EXISTS tmp_norm_amp;
CREATE TEMP TABLE tmp_norm_amp AS
SELECT
    'amplitude'::VARCHAR(16)                                    AS source_system,
    user_id::VARCHAR(64)                                        AS source_id,
    NULL                                                        AS emirates_id,
    NULL::DATE                                                  AS dob,        -- not collected in mobile
    NULL                                                        AS name_english,
    NULL                                                        AS name_arabic,
    NULL                                                        AS phone_e164, -- not collected in mobile
    LOWER(TRIM(email))                                          AS email,
    NULL, NULL, NULL,
    first_seen_at                                               AS created_at,
    last_seen_at                                                AS updated_at
FROM silver.amplitude_user
WHERE email IS NOT NULL;   -- pre-login events are stitched separately via device_id

DROP TABLE IF EXISTS tmp_all_sources;
CREATE TEMP TABLE tmp_all_sources AS
SELECT * FROM tmp_norm_t24
UNION ALL SELECT * FROM tmp_norm_sfdc
UNION ALL SELECT * FROM tmp_norm_amp;

-- -----------------------------------------------------------------------------
-- Step 2: Deterministic matching.
--   Build a candidate pair set. Each row is a (left, right) pair from different
--   source systems that satisfies at least one deterministic rule. We keep the
--   strongest rule and its confidence.
--   D1 has the highest priority; if a pair matches D1 and D2 we record D1.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tmp_deterministic_pairs;
CREATE TEMP TABLE tmp_deterministic_pairs AS
SELECT
    l.source_system   AS left_source,
    l.source_id       AS left_id,
    r.source_system   AS right_source,
    r.source_id       AS right_id,
    'D1'              AS rule_id,
    1.00::NUMERIC(3,2) AS confidence
FROM tmp_all_sources l
JOIN tmp_all_sources r
  ON l.emirates_id = r.emirates_id
 AND l.emirates_id IS NOT NULL
 AND (l.source_system, l.source_id) < (r.source_system, r.source_id)

UNION ALL

SELECT
    l.source_system, l.source_id,
    r.source_system, r.source_id,
    'D2',
    0.95
FROM tmp_all_sources l
JOIN tmp_all_sources r
  ON l.phone_e164 = r.phone_e164
 AND l.dob        = r.dob
 AND l.phone_e164 IS NOT NULL
 AND l.dob        IS NOT NULL
 AND (l.source_system, l.source_id) < (r.source_system, r.source_id)

UNION ALL

SELECT
    l.source_system, l.source_id,
    r.source_system, r.source_id,
    'D3',
    0.90
FROM tmp_all_sources l
JOIN tmp_all_sources r
  ON l.email = r.email
 AND l.dob   = r.dob
 AND l.email IS NOT NULL
 AND l.dob   IS NOT NULL
 AND (l.source_system, l.source_id) < (r.source_system, r.source_id);

-- Collapse to the strongest rule per pair.
DROP TABLE IF EXISTS tmp_det_best;
CREATE TEMP TABLE tmp_det_best AS
SELECT left_source, left_id, right_source, right_id,
       MIN(rule_id)      AS rule_id,     -- D1 < D2 < D3 lexicographically
       MAX(confidence)   AS confidence
FROM tmp_deterministic_pairs
GROUP BY 1,2,3,4;

-- -----------------------------------------------------------------------------
-- Step 3: Assign mal_customer_id via connected components (iterative).
--   Redshift doesn't have graph primitives, so we approximate with a fixed-point
--   loop: for each source record, take the min neighbor id, iterate until stable.
--   Two iterations cover >99% of the sample data. Cap at 5 to bound runtime.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tmp_component;
CREATE TEMP TABLE tmp_component AS
SELECT source_system, source_id,
       source_system || ':' || source_id AS component_key   -- start: each node = own component
FROM tmp_all_sources;

-- One iteration (repeat this block 5x or wrap in a stored procedure).
UPDATE tmp_component c
SET component_key = LEAST(c.component_key, m.min_neighbor)
FROM (
    SELECT source_system, source_id, MIN(neighbor_key) AS min_neighbor
    FROM (
        SELECT left_source  AS source_system, left_id  AS source_id,
               right_source || ':' || right_id AS neighbor_key
        FROM tmp_det_best
        UNION ALL
        SELECT right_source, right_id,
               left_source || ':' || left_id
        FROM tmp_det_best
        UNION ALL
        SELECT source_system, source_id, source_system || ':' || source_id
        FROM tmp_component
    ) g
    GROUP BY 1,2
) m
WHERE c.source_system = m.source_system
  AND c.source_id     = m.source_id;
-- (Repeat above UPDATE 4 more times in the runbook; omitted here for brevity.)

-- Materialize the component-to-mal_customer_id map. Hash the component key so
-- IDs are stable across runs as long as the component doesn't gain new members.
DROP TABLE IF EXISTS tmp_component_id;
CREATE TEMP TABLE tmp_component_id AS
SELECT component_key,
       'MAL-' || SUBSTRING(MD5(component_key), 1, 12) AS mal_customer_id
FROM (SELECT DISTINCT component_key FROM tmp_component) x;

-- -----------------------------------------------------------------------------
-- Step 4: Probabilistic pass — only for source records that are alone in their
--   component (no deterministic match). Compare each unmatched record against
--   every other unmatched record from a different source system.
--
--   For 500K customers the unmatched pool is small (<10% based on sample), so a
--   cross-join with a blocking key on soundex(name_english) is tractable. If the
--   unmatched pool ever grows past ~200K we'll need to move this pass to Spark
--   with LSH — the SQL here would time out.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tmp_unmatched;
CREATE TEMP TABLE tmp_unmatched AS
SELECT s.*, ci.mal_customer_id
FROM tmp_all_sources s
JOIN tmp_component c   USING (source_system, source_id)
JOIN tmp_component_id ci ON c.component_key = ci.component_key
WHERE c.component_key = s.source_system || ':' || s.source_id;   -- singleton = no det match

DROP TABLE IF EXISTS tmp_prob_pairs;
CREATE TEMP TABLE tmp_prob_pairs AS
WITH pairs AS (
    SELECT
        l.source_system AS left_source,  l.source_id AS left_id,
        r.source_system AS right_source, r.source_id AS right_id,
        -- Jaro-Winkler UDF; see mal-etl/udfs/jaro_winkler.py
        COALESCE(fn_jaro_winkler(l.name_english, r.name_english), 0)  AS jw_name,
        CASE
            WHEN l.dob IS NULL OR r.dob IS NULL THEN 0
            WHEN l.dob = r.dob THEN 1.0
            WHEN ABS(DATEDIFF(day, l.dob, r.dob)) <= 180 THEN 0.6
            ELSE 0
        END                                                            AS dob_score,
        CASE
            WHEN l.phone_e164 IS NULL OR r.phone_e164 IS NULL THEN 0
            WHEN l.phone_e164 = r.phone_e164 THEN 1.0
            WHEN RIGHT(l.phone_e164, 6) = RIGHT(r.phone_e164, 6) THEN 0.7
            ELSE 0
        END                                                            AS phone_score,
        CASE
            WHEN l.email IS NULL OR r.email IS NULL THEN 0
            WHEN SPLIT_PART(l.email, '@', 2) = SPLIT_PART(r.email, '@', 2) THEN 1.0
            ELSE 0
        END                                                            AS email_domain_score
    FROM tmp_unmatched l
    JOIN tmp_unmatched r
      ON l.source_system <> r.source_system
     AND (l.source_system, l.source_id) < (r.source_system, r.source_id)
     -- Blocking: only consider pairs whose name soundex matches. Cheap filter,
     -- drops the cross-join by ~95%. Athena: use soundex() from athena-udfs.
     AND SOUNDEX(l.name_english) = SOUNDEX(r.name_english)
)
SELECT
    left_source, left_id, right_source, right_id,
    (0.40 * jw_name
   + 0.30 * dob_score
   + 0.20 * phone_score
   + 0.10 * email_domain_score)::NUMERIC(4,3) AS confidence
FROM pairs;

-- -----------------------------------------------------------------------------
-- Step 5: Split probabilistic pairs by threshold.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS tmp_prob_auto;
CREATE TEMP TABLE tmp_prob_auto AS
SELECT * FROM tmp_prob_pairs WHERE confidence >= 0.70;

DROP TABLE IF EXISTS tmp_prob_review;
CREATE TEMP TABLE tmp_prob_review AS
SELECT * FROM tmp_prob_pairs WHERE confidence >= 0.50 AND confidence < 0.70;

-- Pairs with confidence < 0.50 are implicitly dropped; each side keeps its
-- singleton mal_customer_id from Step 3.

-- -----------------------------------------------------------------------------
-- Step 6: Rebuild the crosswalk.
--   Deterministic matches keep their strongest rule. Probabilistic auto-links
--   append with match_method = 'probabilistic_auto'. New customers (no match at
--   all) land as singleton rows with match_method = 'new_customer'.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE gold.identity_crosswalk;

INSERT INTO gold.identity_crosswalk (
    mal_customer_id, source_system, source_id,
    match_method, confidence, resolved_at
)
-- (a) All deterministic components.
SELECT ci.mal_customer_id, c.source_system, c.source_id,
       'deterministic', 1.00, CURRENT_TIMESTAMP
FROM tmp_component c
JOIN tmp_component_id ci USING (component_key)
WHERE EXISTS (
    SELECT 1 FROM tmp_det_best d
    WHERE (d.left_source, d.left_id)  = (c.source_system, c.source_id)
       OR (d.right_source, d.right_id) = (c.source_system, c.source_id)
)

UNION ALL

-- (b) Probabilistic auto-links: right side inherits left side's mal_customer_id.
SELECT
    left_ci.mal_customer_id,
    p.right_source, p.right_id,
    'probabilistic_auto', p.confidence, CURRENT_TIMESTAMP
FROM tmp_prob_auto p
JOIN tmp_component lc
  ON (lc.source_system, lc.source_id) = (p.left_source, p.left_id)
JOIN tmp_component_id left_ci ON lc.component_key = left_ci.component_key

UNION ALL

-- (c) New customers: everything else that hasn't been placed above.
SELECT ci.mal_customer_id, c.source_system, c.source_id,
       'new_customer', 0.00, CURRENT_TIMESTAMP
FROM tmp_component c
JOIN tmp_component_id ci USING (component_key)
WHERE NOT EXISTS (
    SELECT 1 FROM tmp_det_best d
    WHERE (d.left_source, d.left_id)  = (c.source_system, c.source_id)
       OR (d.right_source, d.right_id) = (c.source_system, c.source_id)
)
AND NOT EXISTS (
    SELECT 1 FROM tmp_prob_auto p
    WHERE (p.left_source,  p.left_id)  = (c.source_system, c.source_id)
       OR (p.right_source, p.right_id) = (c.source_system, c.source_id)
);

-- -----------------------------------------------------------------------------
-- Step 7: Push borderline pairs to the review queue. Stewards work these in the
--         Ops UI; on resolution they call fn_apply_review_decision which updates
--         gold.identity_crosswalk directly. See mal-ops-ui/README.md.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE gold.identity_review_queue;

INSERT INTO gold.identity_review_queue (
    left_source, left_id, right_source, right_id,
    confidence, queued_at, status
)
SELECT left_source, left_id, right_source, right_id,
       confidence, CURRENT_TIMESTAMP, 'pending'
FROM tmp_prob_review;

-- -----------------------------------------------------------------------------
-- Step 8: Survivorship — build dim_customer from the crosswalk + source records.
--         Rules are documented in README.md § Survivorship.
--
--         The idea: for each mal_customer_id, aggregate across all linked source
--         rows and pick a winning value per attribute according to the rule.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE gold.dim_customer;

INSERT INTO gold.dim_customer (
    mal_customer_id,
    emirates_id,
    name_english,
    name_arabic,
    dob,
    email,
    phone_e164,
    kyc_status,
    risk_rating,
    segment,
    first_seen_at,
    source_systems,
    last_refreshed_at
)
WITH linked AS (
    SELECT x.mal_customer_id, s.*
    FROM gold.identity_crosswalk x
    JOIN tmp_all_sources s USING (source_system, source_id)
),
t24_pick AS (   -- T24 wins for regulatory fields
    SELECT mal_customer_id,
           MAX(emirates_id)  AS emirates_id,
           MAX(kyc_status)   AS kyc_status,
           MAX(risk_rating)  AS risk_rating,
           MAX(name_english) AS name_english_t24,
           MAX(name_arabic)  AS name_arabic
    FROM linked WHERE source_system = 't24'
    GROUP BY 1
),
verified_contact AS ( -- Most-recent verified email/phone
    SELECT mal_customer_id,
           FIRST_VALUE(email) OVER (
               PARTITION BY mal_customer_id
               ORDER BY updated_at DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS email,
           FIRST_VALUE(phone_e164) OVER (
               PARTITION BY mal_customer_id
               ORDER BY updated_at DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS phone_e164
    FROM linked
    WHERE email IS NOT NULL OR phone_e164 IS NOT NULL
),
seg_pick AS (  -- Highest tier wins
    SELECT mal_customer_id,
           MIN(CASE segment
                    WHEN 'private'   THEN 1
                    WHEN 'priority'  THEN 2
                    WHEN 'affluent'  THEN 3
                    WHEN 'mass'      THEN 4
                    ELSE 5
               END) AS seg_rank,
           MAX(CASE
                    WHEN segment IN ('private','priority','affluent','mass') THEN segment
                    ELSE NULL
               END) AS any_seg
    FROM linked
    GROUP BY 1
),
agg AS (
    SELECT mal_customer_id,
           MIN(created_at)                                   AS first_seen_at,
           LISTAGG(DISTINCT source_system, ',') WITHIN GROUP (ORDER BY source_system) AS source_systems,
           MAX(dob)                                          AS dob   -- DOB should be identical across matched sources; MAX is a safety net
    FROM linked
    GROUP BY 1
)
SELECT
    a.mal_customer_id,
    t.emirates_id,
    COALESCE(t.name_english_t24, MAX(l.name_english)) AS name_english,
    t.name_arabic,
    a.dob,
    v.email,
    v.phone_e164,
    t.kyc_status,
    t.risk_rating,
    CASE s.seg_rank
        WHEN 1 THEN 'private'
        WHEN 2 THEN 'priority'
        WHEN 3 THEN 'affluent'
        WHEN 4 THEN 'mass'
        ELSE NULL
    END                                                AS segment,
    a.first_seen_at,
    a.source_systems,
    CURRENT_TIMESTAMP                                  AS last_refreshed_at
FROM agg a
LEFT JOIN t24_pick        t USING (mal_customer_id)
LEFT JOIN (SELECT DISTINCT mal_customer_id, email, phone_e164 FROM verified_contact) v
       USING (mal_customer_id)
LEFT JOIN seg_pick        s USING (mal_customer_id)
LEFT JOIN linked          l USING (mal_customer_id)
GROUP BY
    a.mal_customer_id, t.emirates_id, t.name_english_t24, t.name_arabic,
    a.dob, v.email, v.phone_e164, t.kyc_status, t.risk_rating,
    s.seg_rank, a.first_seen_at, a.source_systems;

COMMIT;

-- -----------------------------------------------------------------------------
-- Step 9: Post-run stats (logged by the wrapping Glue job to CloudWatch).
-- -----------------------------------------------------------------------------
SELECT match_method, COUNT(*) AS n
FROM gold.identity_crosswalk
GROUP BY 1
ORDER BY 1;

SELECT COUNT(*) AS review_queue_size FROM gold.identity_review_queue WHERE status = 'pending';

SELECT COUNT(*) AS golden_customer_count FROM gold.dim_customer;
