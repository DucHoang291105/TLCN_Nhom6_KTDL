"""Pure-Python rules for Silver listing features.

The Spark job applies exactly these functions through ``mapPartitions`` so the
behaviour in production is the behaviour covered by unit tests. Rules only read
the canonical ``title``; ``title_has_* = FALSE`` therefore means "not stated in
the title", never "the property does not have it".
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from typing import Any

FEATURE_RULE_VERSION = "listing_feature_rules_v2"

MODEL_CATEGORIES = ("nha_pho", "can_ho", "biet_thu", "dat", "phong_tro_khac", "khong_ro")

# Matched on folded text (lowercase, no diacritics, punctuation -> space).
# Order matters: the first matching group wins, so specific labels such as
# "shophouse" or "căn hộ" are checked before the broad "nhà"/"đất" labels.
MODEL_CATEGORY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("can_ho", (
        r"can ho", r"chung cu", r"condotel", r"officetel", r"offictel",
        r"apartment", r"penthouse", r"duplex", r"tap the",
    )),
    ("biet_thu", (r"biet thu", r"villa", r"lien ke", r"shop ?house", r"nha vuon")),
    ("phong_tro_khac", (
        r"phong tro", r"nha tro", r"phong", r"kho", r"xuong", r"mat bang", r"van phong",
        r"thuong mai", r"commercial", r"khach san", r"cua hang", r"ki ot", r"kiot",
    )),
    ("dat", (r"dat", r"land")),
    ("nha_pho", (
        r"nha o", r"nha pho", r"nha rieng", r"nha mat", r"nha hem", r"nha ngo",
        r"nha nguyen can", r"nha cap 4", r"house", r"nha",
    )),
)

# Titles are noisier than category labels and rely on agent shorthand
# ("CH", "CC", "2PN", "MT", "lô"), so the fallback uses its own ordered rules.
TITLE_FALLBACK_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("biet_thu", (r"biet thu", r"villa", r"lien ke", r"shop ?house", r"lk")),
    ("phong_tro_khac", (
        r"chdv", r"can ho dich vu", r"phong tro", r"nha tro", r"kho", r"xuong",
        r"mat bang", r"van phong", r"building", r"khach san",
    )),
    ("can_ho", (
        r"can ho", r"chung cu", r"ch", r"cc", r"condotel", r"officetel",
        r"penthouse", r"duplex", r"can (?:goc )?\d+ ?pn", r"studio",
    )),
    ("dat", (r"dat", r"lo", r"tho cu", r"dat nen", r"nen")),
    ("nha_pho", (
        r"nha", r"mat tien", r"mt", r"hem", r"hxh", r"ngo", r"\d+ ?(?:tang|lau)",
        r"tret", r"lau",
    )),
    ("can_ho", (r"\d+ ?pn",)),
)

# "hong" is deliberately excluded: it is the folded form of "hồng" in "sổ hồng".
NEGATION_TOKENS = {"khong", "ko", "k", "kh", "chua", "thieu"}
# A frontage mention such as "cách mặt tiền 20m" describes proximity, not the
# property itself.
PROXIMITY_TOKENS = {"gan", "cach", "sat", "ra", "toi", "den", "vai"}
NEGATION_WINDOW = 2

FEATURE_RULES: dict[str, dict[str, tuple[str, ...]]] = {
    "legal": {
        "positive": (
            r"so hong", r"so do", r"shr", r"so rieng", r"so san",
            r"phap ly (?:ro rang|chuan|day du|sach|minh bach)", r"cong chung",
            r"sang ten", r"so hong rieng", r"so dep", r"so vuong",
        ),
        "negative": (
            r"giay tay", r"vi bang", r"so chung", r"chua (?:co )?so",
            r"khong (?:co )?so", r"cho so", r"dang (?:cho|lam) so",
        ),
        # Abbreviations that only stay unambiguous with Vietnamese diacritics.
        "accented": (r"sđ", r"sđcc", r"có sổ"),
    },
    "furnished": {
        "positive": (
            r"full noi that", r"noi that (?:cao cap|day du|xin|dep|sang trong|nhap khau|go)",
            r"day du noi that", r"ntcc", r"full nt", r"full do",
            r"tang (?:toan bo |lai |kem )?noi that", r"kem noi that",
            r"fully furnished", r"furnished",
        ),
        "negative": (r"ban giao tho", r"nha tho moi", r"khong noi that", r"noi that co ban"),
        "accented": (),
    },
    "frontage": {
        "positive": (
            r"mat tien", r"mt", r"mtkd", r"mat pho", r"mat duong", r"mat tien kinh doanh",
        ),
        "negative": (),
        "accented": (),
    },
    "elevator": {
        "positive": (r"thang may",),
        "negative": (),
        "accented": (),
    },
    "car_access": {
        "positive": (
            r"hem xe hoi", r"hxh", r"hem o to", r"hem oto", r"ngo o to", r"ngo oto",
            r"(?:o to|oto|xe hoi) (?:vao|do|ngu|dau|tranh|toi|den)",
            r"duong (?:o to|oto|xe hoi)", r"hem xe tai", r"xe hoi tan cua",
            r"o to tan cua", r"oto tan cua",
        ),
        "negative": (),
        "accented": (),
    },
}

# "TM" also abbreviates "thương mại"; accept it as elevator only next to a
# floor count and never in "TM DV" / "TTTM".
ELEVATOR_TM_PATTERN = re.compile(r"(?:^| )tm(?: |$)")
ELEVATOR_TM_EXCLUDE = re.compile(r"(?:^| )tm (?:dv|dich vu)(?: |$)|(?:^| )(?:dat|khu|tttm) tm(?: |$)")
FLOOR_PATTERN = re.compile(r"(?:^| )(?:\d+ ?(?:tang|lau|t)|tang|lau)(?: |$)")

# Folded matches voided when the accented title shows a different word, e.g.
# "sơ đồ" (floor plan) folds to the same text as "sổ đỏ".
FALSE_FRIENDS: dict[str, tuple[str, str]] = {"so do": ("sơ đồ", "sổ đỏ")}

KNOWN_FLAG_WEIGHTS: dict[str, float] = {
    "price_known": 1.0,
    "area_known": 1.0,
    "rooms_known": 1.0,
    "location_known": 1.0,
    "legal_known": 1.0,
}

FEATURE_COLUMNS = [
    "source_id", "source", "dq_status", "is_rent", "record_hash",
    "category_name", "model_category", "model_category_method",
    "title_has_legal", "title_has_furnished", "title_has_frontage",
    "title_has_elevator", "title_has_car_access",
    "price_known", "area_known", "rooms_known", "location_known", "legal_known",
    "feature_completeness_score", "feature_rule_version",
]


def fold_text(value: Any) -> str:
    """Lowercase, strip Vietnamese diacritics and turn punctuation into spaces."""

    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("đ", "d").replace("Đ", "D").casefold()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()


def accented_tokens_text(value: Any) -> str:
    """Lowercase NFC text with punctuation turned into spaces, diacritics kept."""

    if value is None:
        return ""
    text = unicodedata.normalize("NFC", str(value)).casefold()
    return re.sub(r"\s+", " ", re.sub(r"[\W_]+", " ", text)).strip()


def _compile(patterns: tuple[str, ...]) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(rf"(?:^| )(?:{pattern})(?= |$)") for pattern in patterns)


_CATEGORY_INDEX = tuple((name, _compile(patterns)) for name, patterns in MODEL_CATEGORY_RULES)
_TITLE_CATEGORY_INDEX = tuple((name, _compile(patterns)) for name, patterns in TITLE_FALLBACK_RULES)
_FEATURE_INDEX = {
    feature: {kind: _compile(patterns) for kind, patterns in rules.items()}
    for feature, rules in FEATURE_RULES.items()
}


def _preceding_tokens(text: str, start: int) -> list[str]:
    return text[:start].split()[-NEGATION_WINDOW:]


def _is_false_friend(folded_match: str, title: Any) -> bool:
    if folded_match not in FALSE_FRIENDS:
        return False
    other, intended = FALSE_FRIENDS[folded_match]
    accented = accented_tokens_text(title)
    return other in accented and intended not in accented


def feature_status(feature: str, title: Any) -> str | None:
    """Return POSITIVE, NEGATIVE or None (title does not mention the feature)."""

    folded = fold_text(title)
    if not folded:
        return None
    rules = _FEATURE_INDEX[feature]
    if any(pattern.search(folded) for pattern in rules["negative"]):
        return "NEGATIVE"

    negated = False
    for pattern in rules["positive"]:
        for match in pattern.finditer(folded):
            before = _preceding_tokens(folded, match.start())
            if NEGATION_TOKENS.intersection(before):
                negated = True
                continue
            if feature == "frontage" and PROXIMITY_TOKENS.intersection(before):
                continue
            if _is_false_friend(match.group().strip(), title):
                continue
            return "POSITIVE"

    if rules["accented"]:
        accented = accented_tokens_text(title)
        for pattern in rules["accented"]:
            for match in pattern.finditer(accented):
                if NEGATION_TOKENS.intersection(fold_text(accented[: match.start()]).split()[-NEGATION_WINDOW:]):
                    negated = True
                    continue
                return "POSITIVE"

    if feature == "elevator":
        tm = ELEVATOR_TM_PATTERN.search(folded)
        if tm and FLOOR_PATTERN.search(folded) and not ELEVATOR_TM_EXCLUDE.search(folded):
            if NEGATION_TOKENS.intersection(_preceding_tokens(folded, tm.start())):
                negated = True
            else:
                return "POSITIVE"

    return "NEGATIVE" if negated else None


def map_model_category(category_name: Any, title: Any = None) -> tuple[str, str]:
    """Map a canonical category (fallback: title) to (model_category, method)."""

    for text, method, index in (
        (category_name, "CATEGORY_NAME", _CATEGORY_INDEX),
        (title, "TITLE_FALLBACK", _TITLE_CATEGORY_INDEX),
    ):
        folded = fold_text(text)
        if not folded:
            continue
        for name, patterns in index:
            if any(pattern.search(folded) for pattern in patterns):
                return name, method
    return "khong_ro", "UNMAPPED"


def _positive_number(value: Any) -> bool:
    try:
        return value is not None and float(value) > 0
    except (TypeError, ValueError):
        return False


def feature_completeness(flags: Mapping[str, bool]) -> float:
    total = sum(KNOWN_FLAG_WEIGHTS.values())
    score = sum(weight for name, weight in KNOWN_FLAG_WEIGHTS.items() if flags.get(name))
    return round(score / total, 6)


def build_feature_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Build one ``listing_feature`` row from Core + location + observation fields."""

    title = record.get("title")
    model_category, method = map_model_category(record.get("category_name"), title)
    statuses = {feature: feature_status(feature, title) for feature in FEATURE_RULES}
    known = {
        "price_known": _positive_number(record.get("price")),
        "area_known": _positive_number(record.get("area")),
        "rooms_known": _positive_number(record.get("rooms")),
        "location_known": bool(
            record.get("has_coord")
            or record.get("province_name_model")
            or record.get("district_name_model")
        ),
        "legal_known": statuses["legal"] is not None,
    }
    output = {
        "source_id": record.get("source_id"),
        "source": record.get("source"),
        "dq_status": record.get("dq_status"),
        "is_rent": record.get("is_rent"),
        "record_hash": record.get("record_hash"),
        "category_name": record.get("category_name"),
        "model_category": model_category,
        "model_category_method": method,
        **{f"title_has_{feature}": status == "POSITIVE" for feature, status in statuses.items()},
        **known,
        "feature_completeness_score": feature_completeness(known),
        "feature_rule_version": FEATURE_RULE_VERSION,
    }
    return {column: output[column] for column in FEATURE_COLUMNS}
