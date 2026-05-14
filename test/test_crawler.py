import asyncio

from crawl4ai import BFSDeepCrawlStrategy

import company_data_enrichment.crawler as crawler_module
from company_data_enrichment.crawler import (
    BLOCKED_DEEP_CRAWL_PATTERNS,
    build_failure_row,
    build_run_config,
    crawl_records_async,
    flatten_crawl_result,
    iter_browser_windows,
    should_deep_crawl_record,
)


class FakeCrawlResult:
    def __init__(
        self,
        url,
        success=True,
        markdown="",
        html="",
        metadata=None,
        error_message=None,
    ):
        self.url = url
        self.success = success
        self.markdown = markdown
        self.html = html
        self.metadata = metadata or {}
        self.error_message = error_message


def test_build_run_config_uses_bfs_when_deep_crawl_enabled():
    config = build_run_config(
        timeout_ms=12345,
        retry_count=2,
        deep_crawl_enabled=True,
        deep_crawl_max_depth=1,
        deep_crawl_max_pages=5,
        deep_crawl_include_external=False,
    )

    strategy = config.deep_crawl_strategy

    assert config.page_timeout == 12345
    assert config.max_retries == 2
    assert isinstance(strategy, BFSDeepCrawlStrategy)
    assert strategy.max_depth == 1
    assert strategy.max_pages == 5
    assert strategy.include_external is False


def test_build_run_config_keeps_single_page_mode_when_deep_crawl_disabled():
    config = build_run_config(deep_crawl_enabled=False)

    assert config.deep_crawl_strategy is None


def test_blocked_deep_crawl_patterns_skip_document_files():
    assert "*.pdf*" in BLOCKED_DEEP_CRAWL_PATTERNS
    assert "*.docx*" in BLOCKED_DEEP_CRAWL_PATTERNS
    assert "*.xlsx*" in BLOCKED_DEEP_CRAWL_PATTERNS


def test_should_deep_crawl_record_allows_official_when_enabled():
    record = {"source_type": "official"}

    assert should_deep_crawl_record(record, deep_crawl_enabled=True) is True


def test_should_deep_crawl_record_blocks_social_when_enabled():
    record = {"source_type": "social"}

    assert should_deep_crawl_record(record, deep_crawl_enabled=True) is False


def test_should_deep_crawl_record_blocks_official_when_disabled():
    record = {"source_type": "official"}

    assert should_deep_crawl_record(record, deep_crawl_enabled=False) is False


def test_flatten_crawl_result_keeps_seed_url_for_deep_pages():
    record = {
        "canonical_url": "https://example.com",
        "domain": "example.com",
        "source_type": "official",
        "source_col": "website_url",
        "priority": 1,
    }
    results = [
        FakeCrawlResult(
            url="https://example.com",
            metadata={"depth": 0},
        ),
        FakeCrawlResult(
            url="https://example.com/about",
            metadata={"depth": 1},
        ),
    ]

    rows = flatten_crawl_result(record, results, "2026-05-10T00:00:00+00:00")

    assert len(rows) == 2
    assert rows[0]["seed_url"] == "https://example.com"
    assert rows[1]["seed_url"] == "https://example.com"
    assert rows[1]["canonical_url"] == "https://example.com/about"
    assert rows[1]["depth"] == 1
    assert rows[1]["source_type"] == "official"


def test_build_failure_row_keeps_seed_schema():
    record = {
        "canonical_url": "https://bad.example",
        "domain": "bad.example",
        "source_type": "social",
        "source_col": "facebook_url",
        "priority": 2,
    }

    row = build_failure_row(record, "DNS failed")

    assert row["seed_url"] == "https://bad.example"
    assert row["canonical_url"] == "https://bad.example"
    assert row["depth"] == 0
    assert row["success"] is False
    assert row["error_message"] == "DNS failed"


def test_iter_browser_windows_recycles_after_configured_record_count():
    records = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]

    windows = list(iter_browser_windows(records, recycle_every=2))

    assert windows == [
        (1, 1, 2, [{"id": 1}, {"id": 2}]),
        (2, 3, 4, [{"id": 3}, {"id": 4}]),
        (3, 5, 5, [{"id": 5}]),
    ]


def test_crawl_records_async_preserves_results_when_browser_close_fails(monkeypatch, tmp_path):
    class FakeCrawler:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            raise RuntimeError("Browser.close: Connection closed while reading from the driver")

        async def arun(self, url, config):
            return FakeCrawlResult(
                url=url,
                success=True,
                markdown="ok",
                html="<html>ok</html>",
                metadata={"depth": 0},
            )

    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", FakeCrawler)

    rows = asyncio.run(
        crawl_records_async(
            records=[
                {
                    "canonical_url": "https://example.com",
                    "domain": "example.com",
                    "source_type": "official",
                    "source_col": "website_url",
                    "priority": 1,
                }
            ],
            cache_base_dir=str(tmp_path),
        )
    )

    assert len(rows) == 1
    assert rows[0]["success"] is True
    assert rows[0]["canonical_url"] == "https://example.com"


def test_crawl_records_async_recycles_browser_windows(monkeypatch, tmp_path):
    opened_windows = []

    class FakeCrawler:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            opened_windows.append(self)
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def arun(self, url, config):
            return FakeCrawlResult(
                url=url,
                success=True,
                markdown="ok",
                html="<html>ok</html>",
                metadata={"depth": 0},
            )

    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", FakeCrawler)

    rows = asyncio.run(
        crawl_records_async(
            records=[
                {
                    "canonical_url": "https://one.example",
                    "domain": "one.example",
                    "source_type": "official",
                    "source_col": "website_url",
                    "priority": 1,
                },
                {
                    "canonical_url": "https://two.example",
                    "domain": "two.example",
                    "source_type": "official",
                    "source_col": "website_url",
                    "priority": 1,
                },
                {
                    "canonical_url": "https://three.example",
                    "domain": "three.example",
                    "source_type": "official",
                    "source_col": "website_url",
                    "priority": 1,
                },
            ],
            cache_base_dir=str(tmp_path),
            browser_recycle_every=2,
        )
    )

    assert len(opened_windows) == 2
    assert [row["canonical_url"] for row in rows] == [
        "https://one.example",
        "https://two.example",
        "https://three.example",
    ]


def test_crawl_records_async_records_unhandled_task_exception(monkeypatch, tmp_path):
    class FakeCrawler:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def arun(self, url, config):
            if "bad" in url:
                raise RuntimeError("page crashed")

            return FakeCrawlResult(
                url=url,
                success=True,
                markdown="ok",
                html="<html>ok</html>",
                metadata={"depth": 0},
            )

    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", FakeCrawler)

    rows = asyncio.run(
        crawl_records_async(
            records=[
                {
                    "canonical_url": "https://good.example",
                    "domain": "good.example",
                    "source_type": "official",
                    "source_col": "website_url",
                    "priority": 1,
                },
                {
                    "canonical_url": "https://bad.example",
                    "domain": "bad.example",
                    "source_type": "social",
                    "source_col": "youtube_url",
                    "priority": 2,
                },
            ],
            cache_base_dir=str(tmp_path),
        )
    )

    rows_by_url = {row["canonical_url"]: row for row in rows}

    assert rows_by_url["https://good.example"]["success"] is True
    assert rows_by_url["https://bad.example"]["success"] is False
    assert rows_by_url["https://bad.example"]["error_message"] == "page crashed"


def test_crawl_records_async_times_out_hung_seed_url(monkeypatch, tmp_path):
    class FakeCrawler:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def arun(self, url, config):
            await asyncio.sleep(1)
            return FakeCrawlResult(url=url, success=True)

    monkeypatch.setattr(crawler_module, "AsyncWebCrawler", FakeCrawler)

    rows = asyncio.run(
        crawl_records_async(
            records=[
                {
                    "canonical_url": "https://slow.example",
                    "domain": "slow.example",
                    "source_type": "official",
                    "source_col": "website_url",
                    "priority": 1,
                }
            ],
            cache_base_dir=str(tmp_path),
            seed_timeout_ms=10,
        )
    )

    assert len(rows) == 1
    assert rows[0]["success"] is False
    assert rows[0]["canonical_url"] == "https://slow.example"
    assert rows[0]["error_message"] == "Seed crawl timeout after 10ms"
