from company_data_enrichment.geo_signals import (
    country_name_from_iso,
    extract_tld_from_url,
    extract_country_city_from_text,
    infer_strong_country_from_tld,
    infer_country_from_csv_fields,
    normalize_country_to_iso,
)


def test_normalize_country_aliases_to_iso_code():
    assert normalize_country_to_iso("United States") == "US"
    assert normalize_country_to_iso("USA") == "US"
    assert normalize_country_to_iso("U.S.") == "US"
    assert normalize_country_to_iso("US") == "US"
    assert country_name_from_iso("US") == "United States"


def test_infer_country_from_country_specific_tld():
    country_code, method = infer_country_from_csv_fields("dk", "")

    assert country_code == "DK"
    assert method == "tld"


def test_infer_strong_country_from_url_when_tld_missing():
    country_code, tld, source = infer_strong_country_from_tld(
        website_tld="",
        website_url="https://example.co.uk/about",
    )

    assert country_code == "GB"
    assert tld == "co.uk"
    assert source == "website_url"


def test_extract_tld_from_url_prefers_compound_tld():
    assert extract_tld_from_url("https://example.com.pk") == "com.pk"


def test_infer_country_ignores_generic_tld_before_language_signal():
    country_code, method = infer_country_from_csv_fields("com", "da")

    assert country_code == "DK"
    assert method == "language"


def test_extract_country_city_from_text_keyword():
    result = extract_country_city_from_text(
        "Acme is headquartered in Copenhagen, Denmark.",
        title="Acme",
    )

    assert result["country_code"] == "DK"
    assert result["country_name"] == "Denmark"
    assert result["city"] == "Copenhagen"
