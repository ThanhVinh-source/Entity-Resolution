"""
MODULE: MULTI-SOURCE EVIDENCE EXTRACTOR
--> Task: Transform chaotic web content into structured and quantifiable "Evidence".

Core features closely following source code:
1. Multi-Layer Extraction:
- Structured: In-depth extraction from JSON-LD (Organization, Corporation, LocalBusiness).
- Semi-Structured: Extracts Meta tags (OG, Placename, Language) and HTML Title.
- Unstructured: Uses Regex for Email, Phone, Year Founded, and Employee Count from Markdown.
2. Cleaning:
- Removes noise from navigation tags (nav, footer, header) before processing displayed text.
- Normalizes Title to remove suffixes.
3. Deterministic Geo-Mapping: Integrates with geo_signals to convert all geographic clues to ISO-2 standard, directly supporting the step Country match in the ER process.
4. Weighted Confidence Scoring: Apply the default confidence coefficient (JSON-LD 0.90 > Text 0.65) as input for the entity scoring model.
5. Evidence Provenance: Tag the Context (origin URL, crawl depth, source type) for each data field to facilitate explainability.

"""

import json
import re

from bs4 import BeautifulSoup

from company_data_enrichment.geo_signals import (
    country_name_from_iso,
    extract_country_city_from_text,
    is_weak_locale_signal,
    normalize_country_to_iso,
)


EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
PHONE_RE = re.compile(r"(\+?\d[\d\s().-]{7,}\d)")
YEAR_RE = re.compile(r"\b(18\d{2}|19\d{2}|20\d{2})\b")
FOUNDED_RE = re.compile(
    r"\b(?:founded|established|founded in|established in|since)\D{0,20}"
    r"(18\d{2}|19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
EMPLOYEE_RE = re.compile(
    r"\b(?:employees?|staff|team members?|people|workforce)\D{0,20}"
    r"(\d{1,3}(?:[,\s]\d{3})+|\d{1,7})(?:\s*[-+]\s*)?"
    r"(?:employees?|people|staff|team members?|workforce)?\b",
    re.IGNORECASE,
)
EMPLOYEE_TRAILING_RE = re.compile(
    r"\b(\d{1,3}(?:[,\s]\d{3})+|\d{1,7})\s*"
    r"(?:employees?|people|staff|team members?)\b",
    re.IGNORECASE,
)

DEFAULT_VALIDATION_CONFIG = {
    "jsonld_country_confidence": 0.90,
    "text_country_confidence": 0.65,
    "language_country_confidence": 0.55,
    "city_pattern_confidence": 0.60,
}


# PREPROCESSING AND NORMALIZATION FUNCTIONS
# Clean and normalize text values by removing extra whitespace, converting to uppercase, and stripping non-alphanumeric characters to create consistent data for matching and comparison
def clean_text(value):
    if value is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    if cleaned == "":
        return None

    return cleaned

# Clean the title by removing common separators and extra information, which often includes the company name followed by a separator and additional details
def clean_title(value):
    value = clean_text(value)
    if value is None:
        return None

    parts = re.split(r"\s[-|]\s", value)
    return clean_text(parts[0])


# note: Extract a four-digit founding year from structured or text values.
def extract_year(value):
    value = clean_text(value)
    if value is None:
        return None

    match = YEAR_RE.search(value)
    if not match:
        return None

    return match.group(1)


# note: Normalize employee-count values from JSON-LD numbers, ranges, or text snippets.
def clean_employee_count(value):
    if value is None:
        return None

    if isinstance(value, dict):
        for key in ["value", "minValue", "maxValue"]:
            cleaned = clean_employee_count(value.get(key))
            if cleaned:
                return cleaned
        return None

    if isinstance(value, list):
        for item in value:
            cleaned = clean_employee_count(item)
            if cleaned:
                return cleaned
        return None

    text = clean_text(value)
    if text is None:
        return None

    range_match = re.search(
        r"(\d{1,3}(?:[,\s]\d{3})+|\d{1,7})\s*[-–]\s*"
        r"(\d{1,3}(?:[,\s]\d{3})+|\d{1,7})",
        text,
    )
    if range_match:
        return (
            range_match.group(1).replace(" ", "").replace(",", "")
            + "-"
            + range_match.group(2).replace(" ", "").replace(",", "")
        )

    number_match = re.search(r"\d{1,3}(?:[,\s]\d{3})+|\d{1,7}", text)
    if number_match:
        return number_match.group(0).replace(" ", "").replace(",", "")

    return None


# note: Extract a founded-year hint from visible text when no structured value exists.
def extract_year_founded_from_text(text):
    text = clean_text(text)
    if text is None:
        return None

    match = FOUNDED_RE.search(text)
    if match:
        return match.group(1)

    return None


# note: Extract an employee-count hint from visible text.
def extract_employee_count_from_text(text):
    text = clean_text(text)
    if text is None:
        return None

    match = EMPLOYEE_RE.search(text)
    if not match:
        match = EMPLOYEE_TRAILING_RE.search(text)
        if not match:
            return None

    return clean_employee_count(match.group(1))


# Read validation confidence values from config while keeping stable defaults for tests and ad hoc calls.
def confidence_value(validation_config, key):
    if validation_config is None:
        validation_config = {}

    return float(validation_config.get(key, DEFAULT_VALIDATION_CONFIG[key]))


# Read locale policy from validation config without forcing callers to pass the whole pipeline config.
def locale_policy(validation_config):
    if validation_config is None:
        return None

    return validation_config.get("locale_policy")


# Convert any country evidence into both ISO code and canonical country-name fields when possible.
def make_country_fields(canonical_url, country_value, evidence_type, confidence, evidence_text=None):
    rows = []
    country_code = normalize_country_to_iso(country_value)
    country_name = country_name_from_iso(country_code)

    if country_code:
        field = make_field(
            canonical_url,
            "main_country_code",
            country_code,
            evidence_type,
            confidence,
            evidence_text,
        )
        if field:
            rows.append(field)

    if country_name:
        field = make_field(
            canonical_url,
            "main_country",
            country_name,
            evidence_type,
            confidence,
            evidence_text,
        )
        if field:
            rows.append(field)

    elif country_value:
        field = make_field(
            canonical_url,
            "main_country",
            country_value,
            evidence_type,
            confidence,
            evidence_text,
        )
        if field:
            rows.append(field)

    return rows


# Collect metadata text snippets that are useful for location extraction and evidence context.
def collect_meta_contents(soup):
    contents = []

    for tag in soup.find_all("meta"):
        content = clean_text(tag.get("content"))
        if content:
            contents.append(content)

    return contents


# Strip non-content page chrome before extracting visible body text for geo keyword matching.
def clean_visible_text(soup):
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    return clean_text(soup.get_text(" ", strip=True))


# Extract structured location hints from geo, locale, and language metadata.
def extract_geo_from_meta(canonical_url, soup, validation_config):
    rows = []
    city_confidence = confidence_value(validation_config, "city_pattern_confidence")
    text_country_confidence = confidence_value(validation_config, "text_country_confidence")
    language_country_confidence = confidence_value(
        validation_config,
        "language_country_confidence",
    )

    city_tag = soup.find("meta", attrs={"name": "geo.placename"})
    if city_tag and city_tag.get("content"):
        field = make_field(
            canonical_url,
            "main_city",
            city_tag.get("content"),
            "meta_geo",
            city_confidence,
        )
        if field:
            rows.append(field)

    country_tags = [
        soup.find("meta", attrs={"name": "geo.region"}),
        soup.find("meta", attrs={"property": "og:locale"}),
        soup.find("meta", attrs={"name": "language"}),
    ]
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        country_tags.append({"content": html_tag.get("lang"), "evidence_type": "html_lang"})

    for tag in country_tags:
        if not tag:
            continue

        content = tag.get("content")
        evidence_type = tag.get("evidence_type", "meta_geo")
        if is_weak_locale_signal(content, evidence_type, locale_policy(validation_config)):
            continue

        confidence = text_country_confidence
        if evidence_type == "html_lang" or "locale" in str(content).lower():
            confidence = language_country_confidence

        rows.extend(
            make_country_fields(
                canonical_url,
                content,
                evidence_type,
                confidence,
                content,
            )
        )

    return rows

# STRUCTURING EXTRACTED DATA

# Constructs a standardized dictionary for an extracted field, including the canonical URL, field name, extracted value, normalized value, evidence type, confidence score, and evidence text. 
# This structured format allows for consistent handling of extracted data across different sources and types of evidence.
def make_field(canonical_url, field_name, value, evidence_type, confidence, evidence_text=None):
    value = clean_text(value)

    if value is None:
        return None

    return {
        "canonical_url": canonical_url,
        "field_name": field_name,
        "extracted_value": value,
        "normalized_value": normalize_value(value),
        "evidence_type": evidence_type,
        "field_confidence": float(confidence),
        "evidence_text": clean_text(evidence_text) or value,
    }


# Add crawl-level metadata so extracted fields from deep pages can be mapped back to the seed URL and original CSV row.
def attach_crawl_context(field, crawl_row):
    if field is None:
        return None

    field["seed_url"] = crawl_row.get("seed_url") or crawl_row.get("canonical_url")
    field["source_type"] = crawl_row.get("source_type")
    field["source_col"] = crawl_row.get("source_col")
    field["priority"] = crawl_row.get("priority")
    field["depth"] = crawl_row.get("depth")
    return field


# Applies additional normalization to extracted values, such as converting to uppercase and removing non-alphanumeric characters, to create a standardized format that can improve matching and comparison across different records and sources
def normalize_value(value):
    value = clean_text(value)

    if value is None:
        return None

    value = value.upper()
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value

# JSONLD EXTRACTION FUNCTIONS

# Recursively flattens JSON-LD data structures, handling both lists and nested dictionaries, to yield individual items that can be processed for extraction.
# This is necessary because JSON-LD data can be deeply nested and may contain arrays of objects, so flattening allows for easier access to relevant fields regardless of the original structure.
def flatten_jsonld(data):
    if isinstance(data, list):
        for item in data:
            for nested in flatten_jsonld(item):
                yield nested

    elif isinstance(data, dict):
        if "@graph" in data:
            for nested in flatten_jsonld(data["@graph"]):
                yield nested
        else:
            yield data

# Checks if a given JSON-LD item represents an organization by looking for specific types in the "@type" field, which can be either a string or a list of strings.
def is_organization_jsonld(item):
    item_type = item.get("@type")

    if isinstance(item_type, list):
        return any(
            value in ["Organization", "Corporation", "LocalBusiness"]
            for value in item_type
        )

    return item_type in ["Organization", "Corporation", "LocalBusiness"]

# Takes a canonical URL and JSON-LD data, and extracts relevant fields such as company name, legal names, primary email, primary phone, and address components.
# It uses the make_field function to create structured field dictionaries for each extracted value, assigning confidence scores based on the type of information and its source within the JSON-LD.
# note: Extract company and address fields from Organization-like JSON-LD blocks.
def extract_from_jsonld(canonical_url, data, validation_config=None):
    rows = []
    country_confidence = confidence_value(validation_config, "jsonld_country_confidence")

    for item in flatten_jsonld(data):
        if not is_organization_jsonld(item):
            continue

        field = make_field(
            canonical_url,
            "company_name",
            item.get("name"),
            "json_ld",
            0.75,
        )
        if field:
            rows.append(field)

        field = make_field(
            canonical_url,
            "company_legal_names",
            item.get("legalName"),
            "json_ld",
            0.85,
        )
        if field:
            rows.append(field)

        field = make_field(
            canonical_url,
            "primary_email",
            item.get("email"),
            "json_ld",
            0.85,
        )
        if field:
            rows.append(field)

        field = make_field(
            canonical_url,
            "primary_phone",
            item.get("telephone"),
            "json_ld",
            0.80,
        )
        if field:
            rows.append(field)

        field = make_field(
            canonical_url,
            "year_founded",
            extract_year(item.get("foundingDate") or item.get("foundingYear")),
            "json_ld",
            0.85,
            item.get("foundingDate") or item.get("foundingYear"),
        )
        if field:
            rows.append(field)

        field = make_field(
            canonical_url,
            "employee_count",
            clean_employee_count(item.get("numberOfEmployees")),
            "json_ld",
            0.80,
            item.get("numberOfEmployees"),
        )
        if field:
            rows.append(field)

        address = item.get("address")
        if isinstance(address, dict):
            rows.extend(
                make_country_fields(
                    canonical_url,
                    address.get("addressCountry"),
                    "json_ld",
                    country_confidence,
                )
            )

            mapping = {
                "addressRegion": "main_region",
                "addressLocality": "main_city",
                "postalCode": "main_postcode",
                "streetAddress": "main_street",
            }

            for source_key, field_name in mapping.items():
                field = make_field(
                    canonical_url,
                    field_name,
                    address.get(source_key),
                    "json_ld",
                    0.80,
                )
                if field:
                    rows.append(field)

        elif address:
            rows.extend(
                make_country_fields(
                    canonical_url,
                    address,
                    "json_ld",
                    country_confidence,
                )
            )

        direct_country = item.get("addressCountry")
        if direct_country:
            rows.extend(
                make_country_fields(
                    canonical_url,
                    direct_country,
                    "json_ld",
                    country_confidence,
                )
            )

    return rows

# MAIN EXTRACTION FUNCTION
# Takes a crawl row containing the canonical URL, HTML content, and markdown content, and extracts relevant fields such as company name, short description, primary email, and primary phone number.
# Get the company name from the Title, description and name from the Meta tags, call extract_from_jsonld to retrieve structured data, use Regex to find the Email and Phone number in plain text (Markdown).
# Uses BeautifulSoup to parse the HTML and extract information from the title tag, meta tags
# note: Extract structured company evidence from a crawled HTML/markdown page.
def extract_from_html(crawl_row, validation_config=None):
    canonical_url = crawl_row["canonical_url"]
    html = crawl_row.get("html")
    markdown = crawl_row.get("markdown")

    if not isinstance(html, str):
        html = ""

    if not isinstance(markdown, str):
        markdown = ""

    rows = []

    soup = BeautifulSoup(html, "lxml")
    title_text = None

    if soup.title and soup.title.string:
        title_text = soup.title.string
        field = make_field(
            canonical_url,
            "company_name",
            clean_title(title_text),
            "html_title",
            0.55,
        )
        if field:
            rows.append(field)

    meta_names = [
        ("description", "name"),
        ("og:description", "property"),
        ("og:site_name", "property"),
    ]

    for meta_name, attr_name in meta_names:
        tag = soup.find("meta", attrs={attr_name: meta_name})
        if not tag:
            continue

        content = tag.get("content")
        if not content:
            continue

        if meta_name == "og:site_name":
            field_name = "company_name"
            confidence = 0.60
        else:
            field_name = "short_description"
            confidence = 0.55

        field = make_field(
            canonical_url,
            field_name,
            content,
            "meta",
            confidence,
        )
        if field:
            rows.append(field)

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "{}")
            rows.extend(extract_from_jsonld(canonical_url, data, validation_config))
        except Exception:
            continue

    rows.extend(extract_geo_from_meta(canonical_url, soup, validation_config))

    metadata_text = " ".join(collect_meta_contents(soup))
    visible_text = clean_visible_text(soup)
    geo_text = " ".join(
        value
        for value in [metadata_text, visible_text, markdown]
        if value is not None and str(value).strip() != ""
    )
    geo = extract_country_city_from_text(geo_text, title_text, canonical_url)

    if geo["country_code"]:
        rows.extend(
            make_country_fields(
                canonical_url,
                geo["country_code"],
                "page_text_country",
                confidence_value(validation_config, "text_country_confidence"),
                geo_text,
            )
        )

    if geo["city"]:
        field = make_field(
            canonical_url,
            "main_city",
            geo["city"],
            "page_text_city",
            confidence_value(validation_config, "city_pattern_confidence"),
            geo_text,
        )
        if field:
            rows.append(field)

    founded_year = extract_year_founded_from_text(geo_text)
    if founded_year:
        field = make_field(
            canonical_url,
            "year_founded",
            founded_year,
            "page_text",
            0.65,
            geo_text,
        )
        if field:
            rows.append(field)

    employee_count = extract_employee_count_from_text(geo_text)
    if employee_count:
        field = make_field(
            canonical_url,
            "employee_count",
            employee_count,
            "page_text",
            0.60,
            geo_text,
        )
        if field:
            rows.append(field)

    email_match = EMAIL_RE.search(markdown)
    if email_match:
        field = make_field(
            canonical_url,
            "primary_email",
            email_match.group(0),
            "page_text",
            0.70,
        )
        if field:
            rows.append(field)

    phone_match = PHONE_RE.search(markdown)
    if phone_match:
        field = make_field(
            canonical_url,
            "primary_phone",
            phone_match.group(0),
            "page_text",
            0.60,
        )
        if field:
            rows.append(field)

    return [attach_crawl_context(field, crawl_row) for field in rows]

# Receives a list of all crawled pages, filters out failed attempts, and compiles all extracted data into a flat list ready to be placed into a DataFrame.
# note: Extract fields from every crawl result row that has either a success flag or usable page content.
def extract_rows(crawl_rows, validation_config=None):
    extracted = []

    for crawl_row in crawl_rows:
        has_html = isinstance(crawl_row.get("html"), str) and crawl_row.get("html").strip() != ""
        has_markdown = isinstance(crawl_row.get("markdown"), str) and crawl_row.get("markdown").strip() != ""

        if not crawl_row.get("success") and not has_html and not has_markdown:
            continue

        extracted.extend(extract_from_html(crawl_row, validation_config))

    return extracted
