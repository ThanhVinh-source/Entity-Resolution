from company_data_enrichment.url_manifest import extract_domain, normalize_url


def test_normalize_url_adds_scheme():
    assert normalize_url("example.com") == "https://example.com"


def test_normalize_url_removes_www_and_trailing_slash():
    assert normalize_url("https://www.example.com/") == "https://example.com"


def test_extract_domain():
    assert extract_domain("https://www.example.com/about") == "example.com"