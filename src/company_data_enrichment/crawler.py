"""
MODULE: ASYNCHRONOUS STEALTH CRAWLER (Crawl4AI Engine)
-->Task: Collect raw data (Markdown, HTML, Metadata) for Entity Resolution.

Operating Mechanism & Main Features:
1. BFS Deep Crawling: Uses a Breadth-First Search strategy to automatically crawl subpages (About, Contact) based on depth and pre-configured maximum pages (max_pages).
2. Smart URL Filtering: The URLPatternFilter actively removes junk pages, security pages (login, signup, cart, privacy), and static file formats (.jpg, .zip) to optimize resources.
3. Concurrency & Stealth:
- Controls flow using Semaphore (max_concurrency) to prevent network congestion or IP blocking.
- Runs Chromium browser in incognito (Headless) mode. Stealth Mode and User Simulation.
4. Robust Data Flattening: Converts crawl results (success or failure) into a consistent flatbed structure, making it easier to integrate into Spark/Pandas.
5. Persistent Caching: Automatically manages the cache in the 'silver' directory, supporting data reuse and minimizing the number of duplicate requests.

"""

import asyncio
import json
import os
from datetime import datetime, timezone

from crawl4ai import AsyncWebCrawler, BFSDeepCrawlStrategy, BrowserConfig, CrawlerRunConfig, FilterChain, URLPatternFilter

BLOCKED_DEEP_CRAWL_PATTERNS = [
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
    "*.jpg*",
    "*.jpeg*",
    "*.png*",
    "*.gif*",
    "*.svg*",
    "*.pdf*",
    "*.doc*",
    "*.docx*",
    "*.xls*",
    "*.xlsx*",
    "*.ppt*",
    "*.pptx*",
    "*.csv*",
    "*.zip*",
    "*.rar*",
    "*.7z*",
    "*registration*",
    "*subscriptions*",
]

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
            patterns=BLOCKED_DEEP_CRAWL_PATTERNS,
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


# Split records into browser lifecycle windows so Chromium can be restarted periodically without changing the crawl output schema.
def iter_browser_windows(records, recycle_every=None):
    records = list(records)

    if not records:
        return

    if recycle_every is None:
        recycle_every = len(records)

    recycle_every = int(recycle_every)

    if recycle_every <= 0:
        recycle_every = len(records)

    for start_index in range(0, len(records), recycle_every):
        end_index = min(start_index + recycle_every, len(records))
        window_number = (start_index // recycle_every) + 1

        yield (
            window_number,
            start_index + 1,
            end_index,
            records[start_index:end_index],
        )


# Asynchronously crawl a list of records with concurrency control, returning a list of results that include the crawl success status, fetched content, and any error messages
# Use async to allow for concurrent crawling of multiple URLs, improving efficiency while respecting the maximum concurrency limit and handling timeouts appropriately
async def crawl_records_async(
    records,
    max_concurrency=5,
    timeout_ms=30000,
    seed_timeout_ms=None,
    retry_count=0,
    cache_base_dir=None,
    browser_recycle_every=None,
    deep_crawl_enabled=False,
    deep_crawl_max_depth=1,
    deep_crawl_max_pages=5,
    deep_crawl_include_external=False,
):
    records = list(records)
    cache_base_dir = resolve_cache_base_dir(cache_base_dir)
    all_results = []

    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=False,
        enable_stealth=True,
        use_managed_browser=False
    )

    for window_number, start_record, end_record, record_window in iter_browser_windows(
        records,
        recycle_every=browser_recycle_every,
    ):
        window_results = None
        semaphore = asyncio.Semaphore(max_concurrency)
        active_seed_urls = {}
        started_seed_count = 0
        completed_seed_count = 0

        print(f"[BROWSER] Opening window {window_number}: records {start_record}-{end_record}", flush=True)

        crawler_context = AsyncWebCrawler(
            config=browser_config,
            base_directory=cache_base_dir,
        )

        try:
            async with crawler_context as crawler:
                print(f"[BROWSER] Opened window {window_number}", flush=True)

                async def log_window_progress():
                    while True:
                        await asyncio.sleep(60)
                        pending_urls = list(active_seed_urls.values())
                        print(
                            f"[BROWSER] Window {window_number} still running: "
                            f"{completed_seed_count}/{len(record_window)} seed URLs done, "
                            f"{len(pending_urls)} active",
                            flush=True,
                        )

                        for pending_url in pending_urls[:5]:
                            print(f"[BROWSER] Pending seed URL: {pending_url}", flush=True)

                async def crawl_one(record_number, record):
                    nonlocal completed_seed_count, started_seed_count
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
                        started_seed_count += 1
                        active_seed_urls[record_number] = url
                        try:
                            if seed_timeout_ms is not None and int(seed_timeout_ms) > 0:
                                result = await asyncio.wait_for(
                                    crawler.arun(url=url, config=run_config),
                                    timeout=int(seed_timeout_ms) / 1000,
                                )
                            else:
                                result = await crawler.arun(url=url, config=run_config)
                            return flatten_crawl_result(record, result, fetched_at)

                        except asyncio.TimeoutError:
                            return [
                                build_failure_row(
                                    record,
                                    f"Seed crawl timeout after {int(seed_timeout_ms)}ms",
                                )
                            ]

                        except Exception as exc:
                            return [build_failure_row(record, str(exc))]

                        finally:
                            completed_seed_count += 1
                            active_seed_urls.pop(record_number, None)

                tasks = [
                    crawl_one(record_number, record)
                    for record_number, record in enumerate(record_window, start=start_record)
                ]
                print(f"[BROWSER] Crawling window {window_number}: {len(tasks)} seed URLs", flush=True)
                progress_task = asyncio.create_task(log_window_progress())

                try:
                    result_batches = await asyncio.gather(
                        *tasks,
                        return_exceptions=True,
                    )
                finally:
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass

                window_results = []

                for record, result_batch in zip(record_window, result_batches):
                    if isinstance(result_batch, Exception):
                        window_results.append(build_failure_row(record, str(result_batch)))
                    else:
                        window_results.extend(result_batch)

                success_count = sum(1 for row in window_results if row.get("success"))
                failure_count = len(window_results) - success_count
                print(
                    f"[BROWSER] Finished window {window_number}: "
                    f"{len(window_results)} pages, {success_count} success, {failure_count} failed"
                    f", {started_seed_count} seed URLs started"
                    f", {completed_seed_count} seed URLs completed",
                    flush=True,
                )
                print(f"[BROWSER] Closing window {window_number}", flush=True)

            print(f"[BROWSER] Closed window {window_number}", flush=True)

        except Exception as exc:
            print(f"[BROWSER] Window {window_number} failed: {exc}", flush=True)
            if window_results is None:
                window_results = [
                    build_failure_row(record, str(exc))
                    for record in record_window
                ]

        all_results.extend(window_results or [])

    return all_results

# Synchronous wrapper for the asynchronous crawl_records_async function, allowing it to be called in a blocking manner from non-async code while still leveraging the benefits of async crawling under the hood
def crawl_records(
    records,
    max_concurrency=5,
    timeout_ms=30000,
    seed_timeout_ms=None,
    retry_count=0,
    cache_base_dir=None,
    browser_recycle_every=None,
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
            seed_timeout_ms=seed_timeout_ms,
            retry_count=retry_count,
            cache_base_dir=cache_base_dir,
            browser_recycle_every=browser_recycle_every,
            deep_crawl_enabled=deep_crawl_enabled,
            deep_crawl_max_depth=deep_crawl_max_depth,
            deep_crawl_max_pages=deep_crawl_max_pages,
            deep_crawl_include_external=deep_crawl_include_external,
        )
    )
