import asyncio

from crawl4ai import BFSDeepCrawlStrategy

import company_data_enrichment.crawler as crawler_module
from company_data_enrichment.crawler import (
    build_failure_row,
    build_run_config,
    chunk_records,
    crawl_records_async,
    flatten_crawl_result,
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


def test_chunk_records_splits_records_by_batch_size():
    records = [{"id": 1}, {"id": 2}, {"id": 3}]

    batches = list(chunk_records(records, batch_size=2))

    assert batches == [[{"id": 1}, {"id": 2}], [{"id": 3}]]


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


def test_crawl_records_async_continues_after_batch_close_fails(monkeypatch, tmp_path):
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
            ],
            cache_base_dir=str(tmp_path),
            batch_size=1,
        )
    )

    assert [row["canonical_url"] for row in rows] == [
        "https://one.example",
        "https://two.example",
    ]
    assert [row["success"] for row in rows] == [True, True]


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
            batch_size=2,
        )
    )

    rows_by_url = {row["canonical_url"]: row for row in rows}

    assert rows_by_url["https://good.example"]["success"] is True
    assert rows_by_url["https://bad.example"]["success"] is False
    assert rows_by_url["https://bad.example"]["error_message"] == "page crashed"
