from company_data_enrichment.geo_signals import (
    build_country_signal_votes,
    choose_country_vote,
    classify_country_verdict,
    country_name_from_iso,
    extract_tld_from_url,
    extract_country_city_from_text,
    infer_strong_country_from_tld,
    infer_country_from_csv_fields,
    is_weak_locale_signal,
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


def test_weak_locale_signal_is_ignored_for_validation_votes():
    policy = {
        "weak_locale_evidence_types": ["html_lang", "og_locale", "language"],
        "ignored_locale_tags": ["en", "en-US", "en_US", "english"],
        "locale_requires_support": True,
    }
    signals = build_country_signal_votes(
        website_tld="com",
        website_language_code="en-US",
        evidence_records=[],
        locale_policy=policy,
    )

    assert signals == []
    assert is_weak_locale_signal("en-US", "html_lang", policy) is True


def test_weak_locale_signal_is_config_driven():
    signals = build_country_signal_votes(
        website_tld="com",
        website_language_code="fr-FR",
        evidence_records=[],
    )

    assert signals[0]["country_code"] == "FR"


def test_country_vote_prefers_strong_tld_over_weak_language():
    signals = build_country_signal_votes(
        website_tld="dk",
        website_language_code="sv",
        evidence_records=[],
    )
    vote = choose_country_vote(signals)
    verdict, action = classify_country_verdict(
        db_country="US",
        voted_country=vote["voted_country"],
        vote_confidence=vote["vote_confidence"],
        final_method=vote["final_method"],
    )

    assert vote["voted_country"] == "DK"
    assert vote["vote_confidence"] == 0.75
    assert verdict == "CONFLICT_TLD"
    assert action == "CORRECT_COUNTRY"


def test_country_vote_uses_jsonld_as_strong_web_signal():
    signals = build_country_signal_votes(
        website_tld="com",
        website_language_code="",
        evidence_records=[
            {
                "field_name": "main_country_code",
                "extracted_value": "US",
                "evidence_type": "json_ld",
                "source_type": "official",
            }
        ],
    )
    vote = choose_country_vote(signals)

    assert vote["voted_country"] == "US"
    assert vote["final_method"] == "json_ld"
