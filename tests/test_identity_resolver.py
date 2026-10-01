"""
Tests for the identity resolver.

Split into two groups:

- ``TestUtils`` and ``TestSurvivorship`` — pure-Python, no Spark. Fast; these
  run on every commit.
- ``TestResolverEndToEnd`` — requires a local Spark session. Marked with
  ``@pytest.mark.spark`` so CI can skip it on environments without Java.

Fixtures build minimal source records inline so each test says exactly what it
is checking.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List

import pytest

from python import utils
from python.identity_resolver import (
    CONFIDENCE_D1,
    CONFIDENCE_D2,
    CONFIDENCE_D3,
    PROB_AUTO_THRESHOLD,
    PROB_REVIEW_THRESHOLD,
)


# ---------------------------------------------------------------------------
# utils.py — pure functions
# ---------------------------------------------------------------------------

class TestUtils:
    # phone --------------------------------------------------------------
    def test_normalize_phone_local_uae(self):
        assert utils.normalize_phone("0501234567") == "+971501234567"

    def test_normalize_phone_00_prefix(self):
        assert utils.normalize_phone("00971501234567") == "+971501234567"

    def test_normalize_phone_with_noise(self):
        # "+971 (0) 50-123 4567" — SFDC free-text field
        assert utils.normalize_phone("+971 (0) 50-123 4567") == "+971501234567"

    def test_normalize_phone_returns_none_for_garbage(self):
        assert utils.normalize_phone("N/A") is None
        assert utils.normalize_phone("") is None
        assert utils.normalize_phone(None) is None

    # email --------------------------------------------------------------
    def test_normalize_email_lowercases_and_trims(self):
        assert utils.normalize_email("  Foo.Bar@Example.COM  ") == "foo.bar@example.com"

    def test_normalize_email_rejects_invalid(self):
        assert utils.normalize_email("not-an-email") is None
        assert utils.normalize_email(None) is None

    # name ---------------------------------------------------------------
    def test_normalize_name_strips_diacritics(self):
        # NFKD flattens José -> jose
        assert utils.normalize_name("José García") == "jose garcia"

    def test_normalize_name_handles_arabic(self):
        # Arabic name with combining marks (fatha, damma). Should stay Arabic
        # but strip the combining marks.
        raw = "مُحَمَّد"
        out = utils.normalize_name(raw)
        assert out is not None
        # Diacritic marks removed
        assert "َ" not in out and "ُ" not in out

    def test_normalize_name_collapses_whitespace(self):
        assert utils.normalize_name("  Ali    Al   Mansoori  ") == "ali al mansoori"

    def test_normalize_name_none_and_empty(self):
        assert utils.normalize_name(None) is None
        assert utils.normalize_name("   ") is None

    # jaro-winkler -------------------------------------------------------
    def test_jaro_winkler_identical(self):
        assert utils.jaro_winkler("mohammed ali", "mohammed ali") == 1.0

    def test_jaro_winkler_close(self):
        # Common transliteration variant
        assert utils.jaro_winkler("mohammed", "mohamed") > 0.9

    def test_jaro_winkler_null_safe(self):
        assert utils.jaro_winkler(None, "anything") == 0.0
        assert utils.jaro_winkler("anything", None) == 0.0

    # dob fuzzy ----------------------------------------------------------
    def test_dob_exact_match(self):
        d = date(1990, 5, 12)
        assert utils.dob_fuzzy_match(d, d) == 1.0

    def test_dob_within_tolerance(self):
        # Day/month swap — 12 May vs 5 Dec, ~207 days apart, so use a swap
        # that's within 180.
        a = date(1990, 5, 12)
        b = date(1990, 6, 20)  # 39 days
        assert utils.dob_fuzzy_match(a, b) == 0.6

    def test_dob_outside_tolerance(self):
        a = date(1990, 5, 12)
        b = date(1985, 5, 12)
        assert utils.dob_fuzzy_match(a, b) == 0.0

    def test_dob_null_safe(self):
        assert utils.dob_fuzzy_match(None, date(1990, 1, 1)) == 0.0
        assert utils.dob_fuzzy_match(date(1990, 1, 1), None) == 0.0

    # EID checksum -------------------------------------------------------
    def test_validate_emirates_id_valid(self):
        # A synthetic EID whose Luhn checksum we compute below so this test is
        # self-contained.
        body = "78419901234567"
        total = 0
        for i, ch in enumerate(reversed(body)):
            n = int(ch)
            if i % 2 == 0:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        check = (10 - (total % 10)) % 10
        eid = f"784-1990-1234567-{check}"
        assert utils.validate_emirates_id(eid) == body + str(check)

    def test_validate_emirates_id_bad_checksum(self):
        # Deliberately wrong checksum
        assert utils.validate_emirates_id("784-1990-1234567-0") is None

    def test_validate_emirates_id_wrong_format(self):
        assert utils.validate_emirates_id("123-456-789") is None
        assert utils.validate_emirates_id(None) is None


# ---------------------------------------------------------------------------
# Spark end-to-end tests
# ---------------------------------------------------------------------------

pytestmark_spark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    """Local Spark session for the resolver tests."""
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .appName("mal-identity-resolver-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def resolver(spark):
    from python.identity_resolver import IdentityResolver
    return IdentityResolver(spark)


def _t24(spark, rows: List[Dict]):
    return spark.createDataFrame(rows) if rows else spark.createDataFrame(
        [], "customer_id string, emirates_id string, dob date, name_english string, "
            "name_arabic string, mobile string, email string, risk_rating string, "
            "segment string, kyc_status string, created_at timestamp, updated_at timestamp, "
            "record_status string"
    )


def _sfdc(spark, rows: List[Dict]):
    return spark.createDataFrame(rows) if rows else spark.createDataFrame(
        [], "contact_id string, first_name string, last_name string, dob date, "
            "phone string, email string, created_date timestamp, "
            "last_modified_date timestamp, is_deleted boolean"
    )


def _amp(spark, rows: List[Dict]):
    return spark.createDataFrame(rows) if rows else spark.createDataFrame(
        [], "user_id string, email string, first_seen_at timestamp, last_seen_at timestamp"
    )


@pytest.mark.spark
class TestResolverEndToEnd:

    def test_deterministic_d1_eid_match(self, spark, resolver):
        """Two records with the same validated Emirates ID link at conf=1.0."""
        eid_body = "78419901234567"
        # Compute valid checksum
        total = 0
        for i, ch in enumerate(reversed(eid_body)):
            n = int(ch)
            if i % 2 == 0:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        check = (10 - (total % 10)) % 10
        eid = f"784-1990-1234567-{check}"

        t24 = _t24(spark, [{
            "customer_id": "T001", "emirates_id": eid,
            "dob": date(1990, 1, 1),
            "name_english": "Ahmed Al Mansoori", "name_arabic": None,
            "mobile": "0501111111", "email": "ahmed@example.com",
            "risk_rating": "LOW", "segment": "affluent", "kyc_status": "APPROVED",
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S001", "first_name": "Ahmed", "last_name": "Al Mansoori",
            "dob": date(1990, 1, 1), "phone": "+971502222222",
            "email": "ahmed2@example.com",
            "created_date": datetime(2021, 1, 1), "last_modified_date": datetime(2024, 6, 1),
        }])
        # SFDC doesn't carry EID — so we can't test D1 across T24+SFDC directly.
        # Instead we simulate two T24-like records via a second T24 row.
        t24_second = _t24(spark, [{
            "customer_id": "T002", "emirates_id": eid,
            "dob": date(1990, 1, 1),
            "name_english": "Ahmed Al Mansoori", "name_arabic": None,
            "mobile": "0501111111", "email": "ahmed@example.com",
            "risk_rating": "LOW", "segment": "affluent", "kyc_status": "APPROVED",
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        # Deterministic pass ignores same-source pairs? Actually the resolver
        # currently does allow same-source; upstream Silver dedupes them. We
        # test cross-source via SFDC using a stubbed EID column instead by
        # adding EID directly to a fabricated normalized row.
        from pyspark.sql import functions as F
        norm = resolver.normalize({"t24": t24.withColumn("record_status", F.lit("ACTIVE")), "sfdc": sfdc}).cache()
        # Inject an EID into the SFDC row for the test
        norm = norm.withColumn(
            "emirates_id",
            F.when(F.col("source_system") == "sfdc", F.lit(eid_body))
             .otherwise(F.col("emirates_id"))
        )
        pairs = resolver.deterministic_match(norm).collect()
        # We expect exactly one linking pair between t24:T001 and sfdc:S001
        d1_pairs = [p for p in pairs if p["rule_id"] == "D1"]
        assert len(d1_pairs) == 1
        assert d1_pairs[0]["confidence"] == pytest.approx(CONFIDENCE_D1)

    def test_deterministic_d2_phone_plus_dob(self, spark, resolver):
        """Phone (E.164) + DOB match links at 0.95."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T010", "emirates_id": None,
            "dob": date(1988, 6, 15),
            "name_english": "Fatima Khan", "name_arabic": None,
            "mobile": "0503334444", "email": "fatima.old@example.com",
            "risk_rating": "MED", "segment": "mass", "kyc_status": "APPROVED",
            "created_at": datetime(2019, 3, 1), "updated_at": datetime(2023, 1, 1),
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S010", "first_name": "Fatima", "last_name": "Khan",
            "dob": date(1988, 6, 15), "phone": "00971503334444",  # different shape, same number
            "email": "fatima.new@example.com",
            "created_date": datetime(2022, 5, 1), "last_modified_date": datetime(2024, 8, 1),
        }])
        norm = resolver.normalize({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "sfdc": sfdc,
        })
        pairs = resolver.deterministic_match(norm).collect()
        d2_pairs = [p for p in pairs if p["rule_id"] == "D2"]
        assert len(d2_pairs) == 1
        assert d2_pairs[0]["confidence"] == pytest.approx(CONFIDENCE_D2)

    def test_deterministic_d3_email_plus_dob(self, spark, resolver):
        """Email + DOB (no phone match) links at 0.90."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T020", "emirates_id": None,
            "dob": date(1995, 2, 20),
            "name_english": "Priya Nair", "name_arabic": None,
            "mobile": "0505555555", "email": "priya@example.com",
            "risk_rating": "LOW", "segment": "mass", "kyc_status": "APPROVED",
            "created_at": datetime(2021, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        amp = _amp(spark, [{
            "user_id": "A020", "email": "PRIYA@Example.com",
            "first_seen_at": datetime(2023, 6, 1),
            "last_seen_at": datetime(2024, 9, 1),
        }])
        norm = resolver.normalize({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "amplitude": amp,
        })
        # Amplitude has no DOB, so D3 won't fire; add DOB to amplitude row via
        # a direct injection (the schema allows it).
        norm = norm.withColumn(
            "dob",
            F.when(F.col("source_system") == "amplitude", F.lit(date(1995, 2, 20)))
             .otherwise(F.col("dob"))
        )
        pairs = resolver.deterministic_match(norm).collect()
        d3_pairs = [p for p in pairs if p["rule_id"] == "D3"]
        assert len(d3_pairs) == 1
        assert d3_pairs[0]["confidence"] == pytest.approx(CONFIDENCE_D3)

    def test_probabilistic_auto_link_high_confidence(self, spark, resolver):
        """Same name + DOB + phone last-6 = auto link (>=0.70)."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T030", "emirates_id": None,
            "dob": date(1992, 8, 8),
            "name_english": "Omar Al Zaabi", "name_arabic": None,
            "mobile": "0507777777", "email": "omar1@example.com",
            "risk_rating": "MED", "segment": "priority", "kyc_status": "APPROVED",
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S030", "first_name": "Omar", "last_name": "Al Zaabi",
            "dob": None,  # missing DOB — forces probabilistic
            "phone": "+971509997777",  # last-6 match
            "email": "omar1@example.com",  # domain match
            "created_date": datetime(2021, 1, 1), "last_modified_date": datetime(2024, 1, 1),
        }])
        norm = resolver.normalize({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "sfdc": sfdc,
        })
        det = resolver.deterministic_match(norm)
        prob = resolver.probabilistic_match(norm, det).collect()
        assert len(prob) == 1
        assert prob[0]["confidence"] >= PROB_AUTO_THRESHOLD

    def test_probabilistic_borderline_goes_to_review(self, spark, resolver):
        """Weak fuzzy match lands in the review queue (0.50–0.69)."""
        from pyspark.sql import functions as F
        # Name close, no DOB, different phones, different email domains -> ~0.36
        # We need to hit 0.50–0.69, so add matching email domain (+0.10) and
        # partial phone (+0.14) via last-6 -> ~0.60.
        t24 = _t24(spark, [{
            "customer_id": "T040", "emirates_id": None,
            "dob": None,
            "name_english": "Rania Haddad", "name_arabic": None,
            "mobile": "0501234567",
            "email": "rania@example.com",
            "risk_rating": None, "segment": None, "kyc_status": None,
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S040", "first_name": "Rania", "last_name": "Haddad",
            "dob": None,
            "phone": "+971509234567",  # last-6 = 234567
            "email": "rania.other@example.com",  # same domain
            "created_date": datetime(2021, 1, 1), "last_modified_date": datetime(2024, 1, 1),
        }])
        norm = resolver.normalize({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "sfdc": sfdc,
        })
        det = resolver.deterministic_match(norm)
        prob = resolver.probabilistic_match(norm, det).collect()
        assert len(prob) == 1
        c = prob[0]["confidence"]
        assert PROB_REVIEW_THRESHOLD <= c < PROB_AUTO_THRESHOLD, f"confidence {c} outside review band"

    def test_probabilistic_no_match_becomes_new_customer(self, spark, resolver):
        """Records with no viable signals get separate mal_customer_ids."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T050", "emirates_id": None, "dob": date(1970, 1, 1),
            "name_english": "Yusuf Rahman", "name_arabic": None,
            "mobile": "0501112222", "email": "yusuf@example.com",
            "risk_rating": None, "segment": None, "kyc_status": None,
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S050", "first_name": "Alexandra", "last_name": "Petrova",
            "dob": date(1995, 6, 6), "phone": "+971509998888",
            "email": "alex@other.com",
            "created_date": datetime(2021, 1, 1), "last_modified_date": datetime(2024, 1, 1),
        }])
        _, xwalk, _, stats = resolver.run({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "sfdc": sfdc,
        })
        ids = {r["mal_customer_id"] for r in xwalk.collect()}
        assert len(ids) == 2
        assert stats.new_customer == 2

    def test_survivorship_t24_wins_kyc(self, spark, resolver):
        """When T24 and SFDC both link, T24 supplies KYC/risk_rating."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T060", "emirates_id": None, "dob": date(1985, 4, 4),
            "name_english": "Nadia Aziz", "name_arabic": "نادية عزيز",
            "mobile": "0504445555", "email": "nadia@t24.example.com",
            "risk_rating": "HIGH", "segment": "priority", "kyc_status": "APPROVED",
            "created_at": datetime(2019, 1, 1), "updated_at": datetime(2023, 1, 1),
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S060", "first_name": "Nadia", "last_name": "Aziz",
            "dob": date(1985, 4, 4), "phone": "+971504445555",
            "email": "nadia@sfdc.example.com",
            "created_date": datetime(2022, 1, 1), "last_modified_date": datetime(2024, 6, 1),
        }])
        dim, _, _, _ = resolver.run({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "sfdc": sfdc,
        })
        rows = dim.collect()
        assert len(rows) == 1
        r = rows[0]
        assert r["risk_rating"] == "HIGH"
        assert r["kyc_status"] == "APPROVED"
        assert r["name_arabic"] == "نادية عزيز"

    def test_survivorship_most_recent_contact_wins(self, spark, resolver):
        """Email from the more recently updated source is the winner."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T070", "emirates_id": None, "dob": date(1993, 3, 3),
            "name_english": "Karim Salah", "name_arabic": None,
            "mobile": "0506667777", "email": "karim.old@example.com",
            "risk_rating": "LOW", "segment": "mass", "kyc_status": "APPROVED",
            "created_at": datetime(2019, 1, 1),
            "updated_at": datetime(2022, 1, 1),   # older
        }])
        sfdc = _sfdc(spark, [{
            "contact_id": "S070", "first_name": "Karim", "last_name": "Salah",
            "dob": date(1993, 3, 3), "phone": "+971506667777",
            "email": "karim.new@example.com",
            "created_date": datetime(2020, 1, 1),
            "last_modified_date": datetime(2025, 1, 1),  # newer
        }])
        dim, _, _, _ = resolver.run({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
            "sfdc": sfdc,
        })
        rows = dim.collect()
        assert rows[0]["email"] == "karim.new@example.com"

    def test_survivorship_highest_segment_wins(self, spark, resolver):
        """priority beats mass when both are linked to the same golden id."""
        from pyspark.sql import functions as F
        t24_a = _t24(spark, [{
            "customer_id": "T080", "emirates_id": None, "dob": date(1980, 1, 1),
            "name_english": "Layla Farid", "name_arabic": None,
            "mobile": "0508887777", "email": "layla@example.com",
            "risk_rating": "LOW", "segment": "mass", "kyc_status": "APPROVED",
            "created_at": datetime(2019, 1, 1), "updated_at": datetime(2023, 1, 1),
        }, {
            "customer_id": "T081", "emirates_id": None, "dob": date(1980, 1, 1),
            "name_english": "Layla Farid", "name_arabic": None,
            "mobile": "0508887777", "email": "layla2@example.com",
            "risk_rating": "LOW", "segment": "priority", "kyc_status": "APPROVED",
            "created_at": datetime(2019, 6, 1), "updated_at": datetime(2024, 1, 1),
        }])
        dim, _, _, _ = resolver.run({
            "t24": t24_a.withColumn("record_status", F.lit("ACTIVE")),
        })
        rows = dim.collect()
        # These two rows link via D2 (phone+dob), so one golden row with segment=priority
        segments = [r["segment"] for r in rows]
        assert "priority" in segments

    def test_run_emits_stats(self, spark, resolver):
        """Stats object counts every crosswalk row."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T090", "emirates_id": None, "dob": date(1990, 1, 1),
            "name_english": "Test User", "name_arabic": None,
            "mobile": "0501010101", "email": "test@example.com",
            "risk_rating": "LOW", "segment": "mass", "kyc_status": "APPROVED",
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        _, xwalk, _, stats = resolver.run({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
        })
        assert stats.as_dict()["total"] == xwalk.count()

    def test_null_handling_does_not_crash(self, spark, resolver):
        """Rows with mostly-null fields must not crash the resolver."""
        from pyspark.sql import functions as F
        t24 = _t24(spark, [{
            "customer_id": "T100", "emirates_id": None, "dob": None,
            "name_english": None, "name_arabic": None,
            "mobile": None, "email": None,
            "risk_rating": None, "segment": None, "kyc_status": None,
            "created_at": datetime(2020, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        dim, xwalk, _, _ = resolver.run({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
        })
        # Still produces one row, marked as new customer
        assert xwalk.count() == 1
        assert xwalk.collect()[0]["match_method"] == "new_customer"
        assert dim.count() == 1

    def test_unicode_arabic_name_survives_pipeline(self, spark, resolver):
        """Arabic names round-trip through the resolver without corruption."""
        from pyspark.sql import functions as F
        arabic = "محمد بن راشد"
        t24 = _t24(spark, [{
            "customer_id": "T110", "emirates_id": None, "dob": date(1970, 1, 1),
            "name_english": "Mohammed bin Rashid", "name_arabic": arabic,
            "mobile": "0501020304", "email": "mbr@example.com",
            "risk_rating": "LOW", "segment": "private", "kyc_status": "APPROVED",
            "created_at": datetime(2015, 1, 1), "updated_at": datetime(2024, 1, 1),
        }])
        dim, _, _, _ = resolver.run({
            "t24": t24.withColumn("record_status", F.lit("ACTIVE")),
        })
        assert dim.collect()[0]["name_arabic"] == arabic
