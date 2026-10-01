"""
utils.py
--------
Normalization and similarity helpers used by the identity resolver.

Everything here is pure-Python and side-effect free so it can be unit-tested
without a Spark session and shipped as a UDF into Glue.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from typing import Optional

import jellyfish
import phonenumbers


# ---------------------------------------------------------------------------
# Phone
# ---------------------------------------------------------------------------

_UAE_MOBILE_PREFIX = "+971"


def normalize_phone(phone_raw: Optional[str], default_country: str = "AE") -> Optional[str]:
    """Normalize a phone string to E.164 format (e.g. ``+971501234567``).

    We use ``phonenumbers`` because the four UAE-specific shapes in T24 don't
    cover the SFDC free-text phone field, which occasionally carries landlines,
    international numbers and formatting noise ("+971 (0) 50-123 4567").

    Parameters
    ----------
    phone_raw:
        Raw phone string as it lands in Silver. ``None`` and empty strings return
        ``None`` — the caller decides whether that's a problem.
    default_country:
        ISO-3166 alpha-2. Used when the number has no country code. Defaults to
        ``AE`` because ~98% of Mal's book is UAE-resident.

    Returns
    -------
    ``str`` in E.164 form, or ``None`` if the input can't be parsed as a valid
    phone number.
    """
    if not phone_raw or not phone_raw.strip():
        return None

    cleaned = phone_raw.strip()

    # Common data-entry pattern in T24: leading "00" instead of "+".
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]

    try:
        parsed = phonenumbers.parse(cleaned, default_country)
    except phonenumbers.NumberParseException:
        return None

    if not phonenumbers.is_valid_number(parsed):
        return None

    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: Optional[str]) -> Optional[str]:
    """Lowercase, strip whitespace and validate shape.

    Does not do MX or bounce validation — that's a separate quality pipeline.
    Returns ``None`` for anything that clearly isn't an email so we don't join
    on garbage.
    """
    if not email:
        return None
    e = email.strip().lower()
    if not _EMAIL_RE.match(e):
        return None
    return e


# ---------------------------------------------------------------------------
# Name
# ---------------------------------------------------------------------------

# Characters we strip before comparing names. Keep letters (including
# non-Latin) and spaces; drop punctuation, dots, hyphens, digits.
_NAME_STRIP_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_MULTISPACE_RE = re.compile(r"\s+")


def normalize_name(name: Optional[str]) -> Optional[str]:
    """Return a lowercase, diacritic-stripped, whitespace-collapsed name.

    Uses NFKD decomposition so accented Latin characters and combining marks on
    Arabic (fatha, damma, kasra) collapse to their base form. This is the
    minimum needed for Jaro-Winkler to behave sensibly across sources — SFDC
    tends to carry "José" while T24 tends to carry "Jose", and Amplitude often
    has neither because the user picked their own display name.
    """
    if not name:
        return None

    # NFKD splits "é" into "e" + combining acute; the second pass drops the mark.
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))

    stripped = stripped.lower()
    stripped = _NAME_STRIP_RE.sub(" ", stripped)
    stripped = _MULTISPACE_RE.sub(" ", stripped).strip()

    return stripped or None


# ---------------------------------------------------------------------------
# Similarity
# ---------------------------------------------------------------------------

def jaro_winkler(a: Optional[str], b: Optional[str]) -> float:
    """Jaro-Winkler similarity in ``[0, 1]``.

    Returns ``0.0`` if either side is falsy — this is a scoring function, not a
    match assertion, so callers can weight it against other signals without
    branching on ``None``.
    """
    if not a or not b:
        return 0.0
    return float(jellyfish.jaro_winkler_similarity(a, b))


def dob_fuzzy_match(
    dob_a: Optional[date],
    dob_b: Optional[date],
    tolerance_days: int = 180,
) -> float:
    """Score two dates of birth.

    - Exact match:              1.0
    - Within ``tolerance_days``: 0.6
    - Beyond tolerance:          0.0
    - Either side missing:       0.0

    The 0.6 tier covers a common banking-data smell: the customer wrote their
    DOB into T24 correctly, but the SFDC record was created from a scanned
    passport where day/month got swapped. Not confident enough to auto-link on
    its own, but a useful signal when other fields agree.
    """
    if dob_a is None or dob_b is None:
        return 0.0
    if dob_a == dob_b:
        return 1.0
    if abs((dob_a - dob_b).days) <= tolerance_days:
        return 0.6
    return 0.0


# ---------------------------------------------------------------------------
# Emirates ID
# ---------------------------------------------------------------------------

_EID_RE = re.compile(r"^784-?[0-9]{4}-?[0-9]{7}-?[0-9]$")


def validate_emirates_id(eid: Optional[str]) -> Optional[str]:
    """Validate the Emirates ID checksum (Luhn on the 14-digit body) and return
    the canonical hyphen-less form, or ``None`` if invalid.

    Older T24 records sometimes contain an EID that was captured from a scan and
    never re-verified. We drop those from D1 rather than auto-link on a bad ID.
    """
    if not eid:
        return None
    if not _EID_RE.match(eid):
        return None
    digits = eid.replace("-", "")
    if len(digits) != 15:
        return None

    # Emirates ID uses Luhn on the first 14 digits, checksum in position 15.
    body, check = digits[:14], int(digits[14])
    total = 0
    for i, ch in enumerate(reversed(body)):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    if (10 - (total % 10)) % 10 != check:
        return None
    return digits
