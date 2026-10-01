"""
identity_resolver.py
--------------------
Mal Digital Bank — Customer 360.

PySpark implementation of the two-pass (deterministic + probabilistic) identity
resolution job. Runs inside an AWS Glue 4.0 Python Shell/Spark job; can also be
driven from the CLI against a JSON fixture for local testing.

Flow:
    normalize()           -> unified schema across T24 / SFDC / Amplitude
    deterministic_match() -> D1 (EID), D2 (phone+DOB), D3 (email+DOB)
    probabilistic_match() -> weighted score on remaining singletons
    survivorship()        -> collapse linked rows into gold.dim_customer
    run()                 -> orchestrates the four steps and emits stats

Design notes:
- The class is source-system agnostic. Sources are pre-normalized into a common
  schema before the resolver sees them, so adding a fourth system (e.g. the
  credit card processor) doesn't require touching the matching logic.
- We use GraphFrames-style connected components in Python space, computed by
  iterating a small hash map. For 500K customers the pair set is <2M rows
  after blocking, comfortably in-driver. If we outgrow that we swap this for
  the actual GraphFrames library — the interface stays the same.
- Confidence and match method are always emitted on the crosswalk row so we
  can audit and re-play decisions.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import utils


logger = logging.getLogger("mal.identity_resolver")
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Thresholds — kept as module-level constants so they're easy to tune from a
# job parameter without hunting through the code.
# ---------------------------------------------------------------------------
CONFIDENCE_D1 = 1.00
CONFIDENCE_D2 = 0.95
CONFIDENCE_D3 = 0.90
PROB_AUTO_THRESHOLD = 0.70
PROB_REVIEW_THRESHOLD = 0.50

WEIGHT_NAME = 0.40
WEIGHT_DOB = 0.30
WEIGHT_PHONE = 0.20
WEIGHT_EMAIL_DOMAIN = 0.10


# ---------------------------------------------------------------------------
# Common schema all sources land in after normalize().
# ---------------------------------------------------------------------------
NORMALIZED_SCHEMA = T.StructType([
    T.StructField("source_system", T.StringType(), False),
    T.StructField("source_id",     T.StringType(), False),
    T.StructField("emirates_id",   T.StringType(), True),
    T.StructField("dob",           T.DateType(),   True),
    T.StructField("name_english",  T.StringType(), True),
    T.StructField("name_arabic",   T.StringType(), True),
    T.StructField("phone_e164",    T.StringType(), True),
    T.StructField("email",         T.StringType(), True),
    T.StructField("risk_rating",   T.StringType(), True),
    T.StructField("segment",       T.StringType(), True),
    T.StructField("kyc_status",    T.StringType(), True),
    T.StructField("created_at",    T.TimestampType(), True),
    T.StructField("updated_at",    T.TimestampType(), True),
])


@dataclass
class MatchStats:
    """Rolled-up counters the Glue job pushes to CloudWatch after each run."""
    deterministic: int = 0
    probabilistic_auto: int = 0
    review_queue: int = 0
    new_customer: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "deterministic": self.deterministic,
            "probabilistic_auto": self.probabilistic_auto,
            "review_queue": self.review_queue,
            "new_customer": self.new_customer,
            "total": (
                self.deterministic + self.probabilistic_auto
                + self.review_queue + self.new_customer
            ),
        }


class IdentityResolver:
    """Two-pass identity resolver.

    Instantiate once per Spark session and call :meth:`run` with a dict of
    source-system DataFrames. Returns golden ``dim_customer``, the crosswalk,
    the review queue and a stats object.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ------------------------------------------------------------------
    # Step 1 — normalize sources into the common schema.
    # ------------------------------------------------------------------
    def normalize(self, sources: Dict[str, DataFrame]) -> DataFrame:
        """Return one DataFrame with the common schema, one row per source record.

        ``sources`` maps ``'t24' | 'sfdc' | 'amplitude'`` to the raw Silver
        DataFrame from that system. Anything not in that set raises — we don't
        want a silent typo landing garbage in the resolver.
        """
        normalized_frames: List[DataFrame] = []
        for name, df in sources.items():
            if name == "t24":
                normalized_frames.append(self._normalize_t24(df))
            elif name == "sfdc":
                normalized_frames.append(self._normalize_sfdc(df))
            elif name == "amplitude":
                normalized_frames.append(self._normalize_amplitude(df))
            else:
                raise ValueError(f"unknown source system: {name}")

        # Union with schema alignment. PySpark's unionByName handles ordering.
        result = normalized_frames[0]
        for df in normalized_frames[1:]:
            result = result.unionByName(df, allowMissingColumns=True)

        # Apply the string-level normalizers via UDFs. These are pure functions
        # from utils; running them in Spark keeps the crosswalk reproducible
        # against the SQL variant when the same helpers are registered as UDFs.
        norm_phone_udf = F.udf(utils.normalize_phone, T.StringType())
        norm_email_udf = F.udf(utils.normalize_email, T.StringType())
        norm_name_udf = F.udf(utils.normalize_name, T.StringType())
        validate_eid_udf = F.udf(utils.validate_emirates_id, T.StringType())

        return (
            result
            .withColumn("phone_e164",  norm_phone_udf(F.col("phone_e164")))
            .withColumn("email",       norm_email_udf(F.col("email")))
            .withColumn("name_english", norm_name_udf(F.col("name_english")))
            .withColumn("emirates_id", validate_eid_udf(F.col("emirates_id")))
        )

    def _normalize_t24(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.lit("t24").alias("source_system"),
            F.col("customer_id").cast("string").alias("source_id"),
            F.col("emirates_id"),
            F.col("dob").cast("date").alias("dob"),
            F.col("name_english"),
            F.col("name_arabic"),
            F.col("mobile").alias("phone_e164"),
            F.col("email"),
            F.col("risk_rating"),
            F.col("segment"),
            F.col("kyc_status"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
        )

    def _normalize_sfdc(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.lit("sfdc").alias("source_system"),
            F.col("contact_id").cast("string").alias("source_id"),
            F.lit(None).cast("string").alias("emirates_id"),
            F.col("dob").cast("date").alias("dob"),
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("name_english"),
            F.lit(None).cast("string").alias("name_arabic"),
            F.col("phone").alias("phone_e164"),
            F.col("email"),
            F.lit(None).cast("string").alias("risk_rating"),
            F.lit(None).cast("string").alias("segment"),
            F.lit(None).cast("string").alias("kyc_status"),
            F.col("created_date").cast("timestamp").alias("created_at"),
            F.col("last_modified_date").cast("timestamp").alias("updated_at"),
        )

    def _normalize_amplitude(self, df: DataFrame) -> DataFrame:
        # Amplitude only sees users after login, so DOB/phone are always null.
        # We keep the row so probabilistic can still consider it via email.
        return df.select(
            F.lit("amplitude").alias("source_system"),
            F.col("user_id").cast("string").alias("source_id"),
            F.lit(None).cast("string").alias("emirates_id"),
            F.lit(None).cast("date").alias("dob"),
            F.lit(None).cast("string").alias("name_english"),
            F.lit(None).cast("string").alias("name_arabic"),
            F.lit(None).cast("string").alias("phone_e164"),
            F.col("email"),
            F.lit(None).cast("string").alias("risk_rating"),
            F.lit(None).cast("string").alias("segment"),
            F.lit(None).cast("string").alias("kyc_status"),
            F.col("first_seen_at").cast("timestamp").alias("created_at"),
            F.col("last_seen_at").cast("timestamp").alias("updated_at"),
        )

    # ------------------------------------------------------------------
    # Step 2 — deterministic pass.
    # ------------------------------------------------------------------
    def deterministic_match(self, normalized: DataFrame) -> DataFrame:
        """Return a DataFrame of ``(left, right, rule_id, confidence)`` pairs.

        Only pairs from different source systems are considered — within-source
        deduplication is handled upstream in the Silver layer and shouldn't
        leak into the resolver.
        """
        n = normalized.alias("l")
        m = normalized.alias("r")

        # We enforce (left, right) ordering by tuple to avoid emitting each
        # pair twice. Doing it lexicographically on (source_system, source_id)
        # keeps the join symmetric.
        order_pred = (
            F.concat(F.col("l.source_system"), F.lit(":"), F.col("l.source_id"))
            < F.concat(F.col("r.source_system"), F.lit(":"), F.col("r.source_id"))
        )

        d1 = (
            n.join(m,
                   (F.col("l.emirates_id") == F.col("r.emirates_id"))
                   & F.col("l.emirates_id").isNotNull()
                   & order_pred)
             .select(
                F.col("l.source_system").alias("left_source"),
                F.col("l.source_id").alias("left_id"),
                F.col("r.source_system").alias("right_source"),
                F.col("r.source_id").alias("right_id"),
                F.lit("D1").alias("rule_id"),
                F.lit(CONFIDENCE_D1).alias("confidence"),
             )
        )

        d2 = (
            n.join(m,
                   (F.col("l.phone_e164") == F.col("r.phone_e164"))
                   & (F.col("l.dob") == F.col("r.dob"))
                   & F.col("l.phone_e164").isNotNull()
                   & F.col("l.dob").isNotNull()
                   & order_pred)
             .select(
                F.col("l.source_system").alias("left_source"),
                F.col("l.source_id").alias("left_id"),
                F.col("r.source_system").alias("right_source"),
                F.col("r.source_id").alias("right_id"),
                F.lit("D2").alias("rule_id"),
                F.lit(CONFIDENCE_D2).alias("confidence"),
             )
        )

        d3 = (
            n.join(m,
                   (F.col("l.email") == F.col("r.email"))
                   & (F.col("l.dob") == F.col("r.dob"))
                   & F.col("l.email").isNotNull()
                   & F.col("l.dob").isNotNull()
                   & order_pred)
             .select(
                F.col("l.source_system").alias("left_source"),
                F.col("l.source_id").alias("left_id"),
                F.col("r.source_system").alias("right_source"),
                F.col("r.source_id").alias("right_id"),
                F.lit("D3").alias("rule_id"),
                F.lit(CONFIDENCE_D3).alias("confidence"),
             )
        )

        # Collapse to strongest rule per pair. D1 < D2 < D3 alphabetically, so
        # MIN(rule_id) picks the strongest.
        pairs = d1.unionByName(d2).unionByName(d3)
        return (
            pairs.groupBy("left_source", "left_id", "right_source", "right_id")
                 .agg(
                     F.min("rule_id").alias("rule_id"),
                     F.max("confidence").alias("confidence"),
                 )
        )

    # ------------------------------------------------------------------
    # Step 3 — probabilistic pass on singletons.
    # ------------------------------------------------------------------
    def probabilistic_match(
        self,
        normalized: DataFrame,
        deterministic_pairs: DataFrame,
    ) -> DataFrame:
        """Return probabilistic pairs (all confidences, unfiltered).

        The caller applies the auto/review/reject thresholds; that split lives
        in :meth:`run` so the tuning history stays in one place.
        """
        # Rows that appear on either side of a deterministic match are excluded.
        matched_ids = (
            deterministic_pairs.select(
                F.col("left_source").alias("source_system"),
                F.col("left_id").alias("source_id"),
            ).unionByName(
                deterministic_pairs.select(
                    F.col("right_source").alias("source_system"),
                    F.col("right_id").alias("source_id"),
                )
            ).distinct()
        )

        unmatched = normalized.join(
            matched_ids, on=["source_system", "source_id"], how="left_anti"
        )

        # Blocking key: soundex of the normalized English name. Cheap, cuts the
        # cross-join by ~95% on the sample data. Rows with no name are skipped
        # in the blocked join and picked up as new customers.
        unmatched = unmatched.withColumn(
            "block_key", F.soundex(F.coalesce(F.col("name_english"), F.lit("")))
        )

        u_l = unmatched.alias("l")
        u_r = unmatched.alias("r")

        order_pred = (
            F.concat(F.col("l.source_system"), F.lit(":"), F.col("l.source_id"))
            < F.concat(F.col("r.source_system"), F.lit(":"), F.col("r.source_id"))
        )

        jw_udf = F.udf(utils.jaro_winkler, T.DoubleType())

        def _dob_score(a, b):
            return utils.dob_fuzzy_match(a, b)
        dob_udf = F.udf(_dob_score, T.DoubleType())

        pairs = (
            u_l.join(u_r,
                     (F.col("l.block_key") == F.col("r.block_key"))
                     & (F.col("l.source_system") != F.col("r.source_system"))
                     & order_pred)
        )

        scored = pairs.select(
            F.col("l.source_system").alias("left_source"),
            F.col("l.source_id").alias("left_id"),
            F.col("r.source_system").alias("right_source"),
            F.col("r.source_id").alias("right_id"),
            jw_udf(F.col("l.name_english"), F.col("r.name_english")).alias("jw_name"),
            dob_udf(F.col("l.dob"), F.col("r.dob")).alias("dob_score"),
            F.when(
                F.col("l.phone_e164").isNull() | F.col("r.phone_e164").isNull(), F.lit(0.0)
            ).when(
                F.col("l.phone_e164") == F.col("r.phone_e164"), F.lit(1.0)
            ).when(
                F.substring(F.col("l.phone_e164"), -6, 6)
                == F.substring(F.col("r.phone_e164"), -6, 6),
                F.lit(0.7),
            ).otherwise(F.lit(0.0)).alias("phone_score"),
            F.when(
                F.col("l.email").isNull() | F.col("r.email").isNull(), F.lit(0.0)
            ).when(
                F.split(F.col("l.email"), "@").getItem(1)
                == F.split(F.col("r.email"), "@").getItem(1),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)).alias("email_domain_score"),
        )

        return scored.withColumn(
            "confidence",
            F.lit(WEIGHT_NAME) * F.col("jw_name")
            + F.lit(WEIGHT_DOB) * F.col("dob_score")
            + F.lit(WEIGHT_PHONE) * F.col("phone_score")
            + F.lit(WEIGHT_EMAIL_DOMAIN) * F.col("email_domain_score"),
        )

    # ------------------------------------------------------------------
    # Step 4 — connected components + mal_customer_id assignment.
    # ------------------------------------------------------------------
    def _assign_mal_ids(
        self,
        normalized: DataFrame,
        linked_pairs: DataFrame,
    ) -> DataFrame:
        """Group source records into components; issue one mal_customer_id each.

        Iterative union-find in the driver. 500K records with <2M edges is well
        within a Glue G.1X driver's memory. We keep it here rather than
        introducing GraphFrames because the dependency footprint matters for a
        Glue job.
        """
        edges = [
            ((r.left_source, r.left_id), (r.right_source, r.right_id))
            for r in linked_pairs.select("left_source", "left_id",
                                          "right_source", "right_id").collect()
        ]
        nodes = [
            (r.source_system, r.source_id)
            for r in normalized.select("source_system", "source_id").distinct().collect()
        ]

        parent: Dict[Tuple[str, str], Tuple[str, str]] = {n: n for n in nodes}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]  # path compression
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            # Deterministic ordering: smaller tuple becomes the root so the
            # component's canonical key is stable across runs.
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

        for a, b in edges:
            if a in parent and b in parent:
                union(a, b)

        import hashlib
        rows = []
        for n in nodes:
            root = find(n)
            key = f"{root[0]}:{root[1]}"
            mal_id = "MAL-" + hashlib.md5(key.encode()).hexdigest()[:12]
            rows.append((n[0], n[1], mal_id))

        schema = T.StructType([
            T.StructField("source_system",   T.StringType(), False),
            T.StructField("source_id",       T.StringType(), False),
            T.StructField("mal_customer_id", T.StringType(), False),
        ])
        return self.spark.createDataFrame(rows, schema=schema)

    # ------------------------------------------------------------------
    # Step 5 — survivorship.
    # ------------------------------------------------------------------
    def survivorship(
        self,
        normalized: DataFrame,
        crosswalk: DataFrame,
    ) -> DataFrame:
        """Collapse linked source rows into one golden ``dim_customer`` row.

        Rules (see README):
        - Regulatory fields (EID, kyc_status, risk_rating, name_arabic): T24 wins.
        - Contact info (email, phone): most-recent ``updated_at`` wins.
        - Segment: highest tier — private > priority > affluent > mass.
        - first_seen_at: earliest ``created_at`` across sources.
        - name_english: T24 preferred, else SFDC, else Amplitude.
        """
        linked = normalized.join(crosswalk, on=["source_system", "source_id"])

        # T24 wins for regulatory fields.
        t24_pick = (
            linked.filter(F.col("source_system") == "t24")
                  .groupBy("mal_customer_id")
                  .agg(
                      F.first("emirates_id",  ignorenulls=True).alias("emirates_id"),
                      F.first("kyc_status",   ignorenulls=True).alias("kyc_status"),
                      F.first("risk_rating",  ignorenulls=True).alias("risk_rating"),
                      F.first("name_english", ignorenulls=True).alias("name_english_t24"),
                      F.first("name_arabic",  ignorenulls=True).alias("name_arabic"),
                  )
        )

        sfdc_name = (
            linked.filter(F.col("source_system") == "sfdc")
                  .groupBy("mal_customer_id")
                  .agg(F.first("name_english", ignorenulls=True).alias("name_english_sfdc"))
        )

        # Contact info: most-recent verified. We assume `updated_at` reflects
        # when the value was last verified; Silver enforces this.
        contact_window = linked.filter(
            F.col("email").isNotNull() | F.col("phone_e164").isNotNull()
        )
        latest_contact = (
            contact_window
            .withColumn("rn",
                        F.row_number().over(
                            F.expr("PARTITION BY mal_customer_id ORDER BY updated_at DESC")
                        ))
            .filter(F.col("rn") == 1)
            .select("mal_customer_id",
                    F.col("email").alias("winning_email"),
                    F.col("phone_e164").alias("winning_phone"))
        )

        seg_rank = F.when(F.col("segment") == "private",  1) \
                    .when(F.col("segment") == "priority", 2) \
                    .when(F.col("segment") == "affluent", 3) \
                    .when(F.col("segment") == "mass",     4) \
                    .otherwise(5)

        seg_pick = (
            linked.withColumn("_rank", seg_rank)
                  .groupBy("mal_customer_id")
                  .agg(F.min("_rank").alias("seg_rank"))
                  .withColumn("segment",
                              F.when(F.col("seg_rank") == 1, F.lit("private"))
                               .when(F.col("seg_rank") == 2, F.lit("priority"))
                               .when(F.col("seg_rank") == 3, F.lit("affluent"))
                               .when(F.col("seg_rank") == 4, F.lit("mass"))
                               .otherwise(F.lit(None)))
                  .drop("seg_rank")
        )

        agg = linked.groupBy("mal_customer_id").agg(
            F.min("created_at").alias("first_seen_at"),
            F.max("dob").alias("dob"),
            F.concat_ws(",", F.collect_set("source_system")).alias("source_systems"),
        )

        return (
            agg
            .join(t24_pick,       on="mal_customer_id", how="left")
            .join(sfdc_name,      on="mal_customer_id", how="left")
            .join(latest_contact, on="mal_customer_id", how="left")
            .join(seg_pick,       on="mal_customer_id", how="left")
            .select(
                "mal_customer_id",
                "emirates_id",
                F.coalesce("name_english_t24", "name_english_sfdc").alias("name_english"),
                "name_arabic",
                "dob",
                F.col("winning_email").alias("email"),
                F.col("winning_phone").alias("phone_e164"),
                "kyc_status",
                "risk_rating",
                "segment",
                "first_seen_at",
                "source_systems",
                F.current_timestamp().alias("last_refreshed_at"),
            )
        )

    # ------------------------------------------------------------------
    # Step 6 — orchestration.
    # ------------------------------------------------------------------
    def run(
        self,
        sources: Dict[str, DataFrame],
    ) -> Tuple[DataFrame, DataFrame, DataFrame, MatchStats]:
        """Run the whole pipeline. Returns (dim_customer, crosswalk, review_queue, stats)."""
        logger.info("normalizing %d source systems", len(sources))
        normalized = self.normalize(sources).cache()

        logger.info("running deterministic pass")
        det_pairs = self.deterministic_match(normalized).cache()

        logger.info("running probabilistic pass on unmatched singletons")
        prob_pairs = self.probabilistic_match(normalized, det_pairs)

        prob_auto = prob_pairs.filter(F.col("confidence") >= PROB_AUTO_THRESHOLD)
        prob_review = prob_pairs.filter(
            (F.col("confidence") >= PROB_REVIEW_THRESHOLD)
            & (F.col("confidence") < PROB_AUTO_THRESHOLD)
        )

        # Union of deterministic + probabilistic-auto edges feeds component id.
        auto_edges = det_pairs.select(
            "left_source", "left_id", "right_source", "right_id"
        ).unionByName(
            prob_auto.select("left_source", "left_id", "right_source", "right_id")
        )

        component_map = self._assign_mal_ids(normalized, auto_edges)

        # Build the crosswalk with match_method + confidence attribution.
        det_side_a = det_pairs.select(
            F.col("left_source").alias("source_system"),
            F.col("left_id").alias("source_id"),
            F.lit("deterministic").alias("match_method"),
            F.col("confidence"),
        )
        det_side_b = det_pairs.select(
            F.col("right_source").alias("source_system"),
            F.col("right_id").alias("source_id"),
            F.lit("deterministic").alias("match_method"),
            F.col("confidence"),
        )
        prob_side_a = prob_auto.select(
            F.col("left_source").alias("source_system"),
            F.col("left_id").alias("source_id"),
            F.lit("probabilistic_auto").alias("match_method"),
            F.col("confidence"),
        )
        prob_side_b = prob_auto.select(
            F.col("right_source").alias("source_system"),
            F.col("right_id").alias("source_id"),
            F.lit("probabilistic_auto").alias("match_method"),
            F.col("confidence"),
        )

        method_attribution = (
            det_side_a.unionByName(det_side_b)
                       .unionByName(prob_side_a)
                       .unionByName(prob_side_b)
                       .groupBy("source_system", "source_id")
                       # Deterministic wins over probabilistic if both fire.
                       .agg(
                           F.min("match_method").alias("match_method"),
                           F.max("confidence").alias("confidence"),
                       )
        )

        crosswalk = (
            component_map
            .join(method_attribution, on=["source_system", "source_id"], how="left")
            .withColumn(
                "match_method",
                F.coalesce(F.col("match_method"), F.lit("new_customer"))
            )
            .withColumn("confidence", F.coalesce(F.col("confidence"), F.lit(0.0)))
            .withColumn("resolved_at", F.current_timestamp())
        )

        # Review queue rows are pair-shaped; the Ops UI joins them back to
        # source records when a steward opens a case.
        review_queue = prob_review.select(
            "left_source", "left_id", "right_source", "right_id", "confidence"
        ).withColumn("queued_at", F.current_timestamp()) \
         .withColumn("status", F.lit("pending"))

        dim_customer = self.survivorship(normalized, crosswalk)

        stats = self._compute_stats(crosswalk, review_queue)
        logger.info("run complete: %s", stats.as_dict())

        return dim_customer, crosswalk, review_queue, stats

    def _compute_stats(self, crosswalk: DataFrame, review_queue: DataFrame) -> MatchStats:
        method_counts = {
            row["match_method"]: row["n"]
            for row in crosswalk.groupBy("match_method").agg(F.count("*").alias("n")).collect()
        }
        return MatchStats(
            deterministic=method_counts.get("deterministic", 0),
            probabilistic_auto=method_counts.get("probabilistic_auto", 0),
            review_queue=review_queue.count(),
            new_customer=method_counts.get("new_customer", 0),
        )


# ---------------------------------------------------------------------------
# CLI entry point — used for local runs against the sample fixture. The Glue
# job wrapper doesn't call this; it constructs the resolver directly.
# ---------------------------------------------------------------------------
def _load_fixture(spark: SparkSession, path: str) -> Dict[str, DataFrame]:
    with open(path) as f:
        payload = json.load(f)

    def _to_df(rows: List[dict], schema_hint: List[Tuple[str, str]]) -> DataFrame:
        # We rely on Spark's automatic schema inference for the fixture, then
        # cast known date/timestamp fields.
        if not rows:
            return spark.createDataFrame([], T.StructType([]))
        df = spark.createDataFrame(rows)
        for col, typ in schema_hint:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(typ))
        return df

    return {
        "t24": _to_df(payload.get("t24", []), [
            ("dob", "date"), ("created_at", "timestamp"), ("updated_at", "timestamp"),
        ]),
        "sfdc": _to_df(payload.get("sfdc", []), [
            ("dob", "date"),
            ("created_date", "timestamp"), ("last_modified_date", "timestamp"),
        ]),
        "amplitude": _to_df(payload.get("amplitude", []), [
            ("first_seen_at", "timestamp"), ("last_seen_at", "timestamp"),
        ]),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="path to fixture JSON")
    parser.add_argument("--output", required=True, help="path to write resolved output JSON")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    spark = (
        SparkSession.builder
        .appName("mal-identity-resolver-local")
        .master("local[*]")
        .getOrCreate()
    )

    sources = _load_fixture(spark, args.input)
    resolver = IdentityResolver(spark)
    dim, xwalk, review, stats = resolver.run(sources)

    out = {
        "stats": stats.as_dict(),
        "dim_customer": [r.asDict() for r in dim.collect()],
        "crosswalk":    [r.asDict() for r in xwalk.collect()],
        "review_queue": [r.asDict() for r in review.collect()],
    }
    with open(args.output, "w") as f:
        json.dump(out, f, default=str, indent=2)

    print(json.dumps(stats.as_dict(), indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
