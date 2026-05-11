from crawl4ai import BFSDeepCrawlStrategy

from company_data_enrichment.crawler import (
    build_failure_row,
    build_run_config,
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
