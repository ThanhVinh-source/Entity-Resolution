"""
ASYNCHRONOUS SMART CRAWLER (Crawl4AI)

--> Collect raw data (Markdown, HTML, Metadata) from a list of vendor URLs.

Core Features:
1. Deep Crawling (BFS): Automatically crawls subpages (About, Contact) to find more evidence.
2. Smart Filtering: Intelligent URL filtering removes junk pages (Login, Cart, Media, Sign In, Sign Up) to reduce noise.
3. High Performance: Runs asynchronously (Async) with Semaphore control to avoid IP blocking.
4. Robustness: Integrates Caching (resource saving) and Error Handling (detailed error logging).
5. Data Flattening: Standardizes all results into table structure to prepare for Extract and Merge.
"""

import asyncio
import json
import os
from datetime import datetime, timezone

from crawl4ai import AsyncWebCrawler, BFSDeepCrawlStrategy, BrowserConfig, CrawlerRunConfig, FilterChain, URLPatternFilter

# Utility function to safely get an attribute from an object, returning None if any exception occurs 
def safe_getattr(obj, name):
    try:
        return getattr(obj, name)
    except Exception:
        return None

# Resolve the cache base directory from the environment variable or use a default path, ensuring the directory exists
def resolve_cache_base_dir(cache_base_dir=None):
    if cache_base_dir is None:
        cache_base_dir = os.getenv(
            "CRAWL4_AI_BASE_DIRECTORY",
            os.path.join("dataset", "silver", "crawl4ai-cache"),
        )

    cache_base_dir = os.path.abspath(cache_base_dir)
    os.makedirs(cache_base_dir, exist_ok=True)
    os.environ["CRAWL4_AI_BASE_DIRECTORY"] = cache_base_dir
    return cache_base_dir

# Build a failure result dictionary for a given record and error message, marking the crawl as unsuccessful and including the error details
def build_failure_row(record, error_message):
    seed_url = record.get("canonical_url")

    return {
        "seed_url": seed_url,
        "canonical_url": seed_url,
        "domain": record.get("domain"),
        "depth": 0,
        "source_type": record.get("source_type"),
        "source_col": record.get("source_col"),
        "priority": record.get("priority"),
        "success": False,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "markdown": None,
        "html": None,
        "metadata_json": None,
        "error_message": error_message,
    }


# Decide whether a specific crawl queue record should use deep crawling.
def should_deep_crawl_record(record, deep_crawl_enabled=False):
    return deep_crawl_enabled and record.get("source_type") == "official"


# Build a Crawl4AI run configuration, enabling BFS deep crawling only when requested in the project config.
def build_run_config(
    timeout_ms=30000,
    retry_count=0,
    deep_crawl_enabled=False,
    deep_crawl_max_depth=1,
    deep_crawl_max_pages=5,
    deep_crawl_include_external=False,
):
    deep_crawl_strategy = None

    if deep_crawl_enabled:
        blocked_url_filter = URLPatternFilter(
            patterns=[
                "*login*",
                "*signin*",
                "*sign-in*",
                "*sign_in*",
                "*sign up*",
                "*sign-up*",
                "*sign_up*",
                "*signup*",
                "*register*",
                "*account*",
                "*/user/*",
                "*password*",
                "*forgot*",
                "*forgotpassword*",
                "*reset-password*",
                "*cart*",
                "*checkout*",
                "*privacy*",
                "*cookie*",
                "*terms*",
                "*.jpg",
                "*.jpeg",
                "*.png",
                "*.gif",
                "*.zip",
                "*registration*",
                "*subscriptions*"
            ],
            reverse=True,
        )

        filter_chain = FilterChain([blocked_url_filter])

        deep_crawl_strategy = BFSDeepCrawlStrategy(
            max_depth=deep_crawl_max_depth,
            max_pages=deep_crawl_max_pages,
            include_external=deep_crawl_include_external,
            filter_chain=filter_chain,
        )

    return CrawlerRunConfig(
        wait_until="domcontentloaded",
        delay_before_return_html=2.0,
        page_timeout=timeout_ms,
        word_count_threshold=10,
        remove_overlay_elements=True,
        remove_consent_popups=True,
        simulate_user=True,
        override_navigator=True,
        magic=True,
        scan_full_page=True,
        scroll_delay=0.5,
        max_scroll_steps=3,
        max_retries=retry_count,
        deep_crawl_strategy=deep_crawl_strategy,
)


# Flatten either one Crawl4AI result or a deep-crawl list into the raw crawl table schema.
def flatten_crawl_result(record, crawl_result, fetched_at):
    if isinstance(crawl_result, list):
        crawl_results = crawl_result
    else:
        crawl_results = [crawl_result]

    rows = []
    seed_url = record.get("canonical_url")

    for result in crawl_results:
        metadata = safe_getattr(result, "metadata") or {}
        page_url = safe_getattr(result, "url") or seed_url

        rows.append(
            {
                "seed_url": seed_url,
                "canonical_url": page_url,
                "domain": record.get("domain"),
                "depth": metadata.get("depth", 0),
                "source_type": record.get("source_type"),
                "source_col": record.get("source_col"),
                "priority": record.get("priority"),
                "success": bool(safe_getattr(result, "success")),
                "fetched_at": fetched_at,
                "markdown": safe_getattr(result, "markdown"),
                "html": safe_getattr(result, "html") or safe_getattr(result, "cleaned_html"),
                "metadata_json": json.dumps(metadata, ensure_ascii=False, default=str),
                "error_message": safe_getattr(result, "error_message"),
            }
        )

    return rows


# Asynchronously crawl a list of records with concurrency control, returning a list of results that include the crawl success status, fetched content, and any error messages
# Use async to allow for concurrent crawling of multiple URLs, improving efficiency while respecting the maximum concurrency limit and handling timeouts appropriately
async def crawl_records_async(
    records,
    max_concurrency=5,
    timeout_ms=30000,
    retry_count=0,
    cache_base_dir=None,
    deep_crawl_enabled=False,
    deep_crawl_max_depth=1,
    deep_crawl_max_pages=5,
    deep_crawl_include_external=False,
):
    cache_base_dir = resolve_cache_base_dir(cache_base_dir)

    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=False,
        enable_stealth=True,
        use_managed_browser=False
    )

    semaphore = asyncio.Semaphore(max_concurrency)

    try:
        crawler_context = AsyncWebCrawler(config=browser_config, base_directory=cache_base_dir)

        async with crawler_context as crawler:

            async def crawl_one(record):
                url = record["canonical_url"]
                fetched_at = datetime.now(timezone.utc).isoformat()
                run_config = build_run_config(
                    timeout_ms=timeout_ms,
                    retry_count=retry_count,
                    deep_crawl_enabled=should_deep_crawl_record(
                        record,
                        deep_crawl_enabled=deep_crawl_enabled,
                    ),
                    deep_crawl_max_depth=deep_crawl_max_depth,
                    deep_crawl_max_pages=deep_crawl_max_pages,
                    deep_crawl_include_external=deep_crawl_include_external,
                )

                async with semaphore:
                    try:
                        result = await crawler.arun(url=url, config=run_config)
                        return flatten_crawl_result(record, result, fetched_at)

                    except Exception as exc:
                        return [build_failure_row(record, str(exc))]

            tasks = [crawl_one(record) for record in records]
            result_batches = await asyncio.gather(*tasks)
            results = [
                row
                for result_batch in result_batches
                for row in result_batch
            ]

    except Exception as exc:
        results = [build_failure_row(record, str(exc)) for record in records]

    return results

# Synchronous wrapper for the asynchronous crawl_records_async function, allowing it to be called in a blocking manner from non-async code while still leveraging the benefits of async crawling under the hood
def crawl_records(
    records,
    max_concurrency=5,
    timeout_ms=30000,
    retry_count=0,
    cache_base_dir=None,
    deep_crawl_enabled=False,
    deep_crawl_max_depth=1,
    deep_crawl_max_pages=5,
    deep_crawl_include_external=False,
):
    return asyncio.run(
        crawl_records_async(
            records=records,
            max_concurrency=max_concurrency,
            timeout_ms=timeout_ms,
            retry_count=retry_count,
            cache_base_dir=cache_base_dir,
            deep_crawl_enabled=deep_crawl_enabled,
            deep_crawl_max_depth=deep_crawl_max_depth,
            deep_crawl_max_pages=deep_crawl_max_pages,
            deep_crawl_include_external=deep_crawl_include_external,
        )
    )
