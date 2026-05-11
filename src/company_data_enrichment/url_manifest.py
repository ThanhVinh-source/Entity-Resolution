"""
MODULE: URL MANIFEST and CRAWL QUEUE BUILDER
--> Convert raw URLs from CSV into clean and optimized queue lists.

Core Features:
1. URL Normalization: Normalizes links (adds https, lowercase, removes www) to the standard Canonical URL format.
2. Manifest Aggregation: Groups all URLs from multiple columns (Official, Social) into a centralized list.
3. Smart Deduplication: Removes duplicate URLs to prevent bots from crawling the same website again.
4. Priority Logic: Sets the priority order (Official Website > Social Networks) for the crawl queue.
5. Domain Extraction: Extracts clean domains for cache management.
"""

from urllib.parse import urlparse, urlunparse

import pandas as pd


def normalize_url(url):
    if url is None:
        return None

    value = str(url).strip()
    if value == "":
        return None

    if not value.startswith(("http://", "https://")):
        value = "https://" + value

    parsed = urlparse(value)
    scheme = parsed.scheme.lower() or "https"
    domain = parsed.netloc.lower()

    if domain.startswith("www."):
        domain = domain[4:]

    path = parsed.path.rstrip("/")
    normalized = urlunparse((scheme, domain, path, "", "", ""))
    return normalized


def extract_domain(url):
    if url is None:
        return None

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    if domain.startswith("www."):
        domain = domain[4:]

    return domain or None

# Build a manifest of URLs from a DataFrame based on specified URL columns and their source types.
def build_url_manifest(df, url_columns):
    parts = []

    for column_name, source_type in url_columns.items():
        if column_name not in df.columns:
            continue

        part = df[
            ["input_row_key", "Applications_of_AI_id", column_name]
        ].copy()
        part = part.rename(columns={column_name: "raw_url"})
        part["source_col"] = column_name
        part["source_type"] = source_type
        part = part[part["raw_url"].astype(str).str.strip() != ""]
        parts.append(part)

    if len(parts) == 0:
        raise ValueError("No URL columns found in config.")

    manifest = pd.concat(parts, ignore_index=True)
    manifest["canonical_url"] = manifest["raw_url"].apply(normalize_url)
    manifest = manifest[manifest["canonical_url"].notna()].copy()
    manifest["domain"] = manifest["canonical_url"].apply(extract_domain)
    manifest["priority"] = manifest["source_type"].apply(
        lambda value: 1 if value == "official" else 2
    )

    columns = [
        "input_row_key",
        "Applications_of_AI_id",
        "source_col",
        "source_type",
        "raw_url",
        "canonical_url",
        "domain",
        "priority",
    ]
    return manifest[columns]

# Build a crawl queue from the URL manifest by sorting and deduplicating based on canonical URLs.
def build_crawl_queue(manifest_df):
    queue_df = (
        manifest_df.sort_values(["priority", "domain", "canonical_url"])
        .drop_duplicates(subset=["canonical_url"], keep="first")
        .loc[:, ["canonical_url", "domain", "priority", "source_type", "source_col"]]
        .reset_index(drop=True)
    )

    return queue_df