import json

import pandas as pd

from company_data_enrichment.pipeline import (
    build_best_evidence,
    build_csv_geo_evidence,
    build_geo_prefill,
    print_crawl_results_summary,
)


def test_print_crawl_results_summary_prints_source_and_success_counts(capsys):
    results_df = pd.DataFrame(
        [
            {"source_type": "official", "success": True},
            {"source_type": "official", "success": False},
            {"source_type": "social", "success": False},
        ]
    )

    print_crawl_results_summary(results_df)

    output = capsys.readouterr().out

    assert "source_type" in output
    assert "official" in output
    assert "social" in output
    assert "success" in output
    assert "True" in output
    assert "False" in output


def test_print_crawl_results_summary_handles_empty_dataframe(capsys):
    print_crawl_results_summary(pd.DataFrame())

    output = capsys.readouterr().out

    assert "Crawl results summary: no rows." in output


def test_build_best_evidence_joins_deep_page_by_seed_url(tmp_path):
    manifest_df = pd.DataFrame(
        [
            {
                "input_row_key": "0",
                "Applications_of_AI_id": "A1",
                "source_col": "website_url",
                "source_type": "official",
                "raw_url": "https://example.com",
                "canonical_url": "https://example.com",
                "domain": "example.com",
                "priority": 1,
            }
        ]
    )
    extracted_df = pd.DataFrame(
        [
            {
                "seed_url": "https://example.com",
                "canonical_url": "https://example.com/about",
                "source_type": "official",
                "source_col": "website_url",
                "priority": 1,
                "depth": 1,
                "field_name": "short_description",
                "extracted_value": "Industrial automation supplier",
                "normalized_value": "INDUSTRIAL AUTOMATION SUPPLIER",
                "evidence_type": "meta",
                "field_confidence": 0.75,
                "evidence_text": "Industrial automation supplier",
            }
        ]
    )

    manifest_df.to_parquet(tmp_path / "url_manifest.parquet", index=False)
    extracted_df.to_parquet(tmp_path / "crawl_extracted_fields.parquet", index=False)

    result = build_best_evidence({"output_dir": str(tmp_path)})
    evidence = json.loads(result.loc[0, "evidence_json"])

    assert result.loc[0, "Applications_of_AI_id"] == "A1"
    assert result.loc[0, "extracted_short_description"] == "Industrial automation supplier"
    assert evidence[0]["seed_url"] == "https://example.com"
    assert evidence[0]["page_url"] == "https://example.com/about"
    assert evidence[0]["depth"] == 1


def test_build_best_evidence_prefers_shallower_page_on_confidence_tie(tmp_path):
    manifest_df = pd.DataFrame(
        [
            {
                "input_row_key": "0",
                "Applications_of_AI_id": "A1",
                "source_col": "website_url",
                "source_type": "official",
                "raw_url": "https://example.com",
                "canonical_url": "https://example.com",
                "domain": "example.com",
                "priority": 1,
            }
        ]
    )
    extracted_df = pd.DataFrame(
        [
            {
                "seed_url": "https://example.com",
                "canonical_url": "https://example.com/about",
                "depth": 1,
                "field_name": "company_name",
                "extracted_value": "About Page Name",
                "normalized_value": "ABOUT PAGE NAME",
                "evidence_type": "meta",
                "field_confidence": 0.75,
                "evidence_text": "About Page Name",
            },
            {
                "seed_url": "https://example.com",
                "canonical_url": "https://example.com",
                "depth": 0,
                "field_name": "company_name",
                "extracted_value": "Home Page Name",
                "normalized_value": "HOME PAGE NAME",
                "evidence_type": "json_ld",
                "field_confidence": 0.75,
                "evidence_text": "Home Page Name",
            },
        ]
    )

    manifest_df.to_parquet(tmp_path / "url_manifest.parquet", index=False)
    extracted_df.to_parquet(tmp_path / "crawl_extracted_fields.parquet", index=False)

    result = build_best_evidence({"output_dir": str(tmp_path)})

    assert result.loc[0, "extracted_company_name"] == "Home Page Name"


def test_build_geo_prefill_locks_country_code_and_name_from_tld():
    input_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "website_tld": "dk",
                "website_url": "https://example.dk",
                "website_language_code": "",
            }
        ]
    )

    rows = build_geo_prefill(
        input_df,
        {
            "enabled": True,
            "tld_country_confidence": 0.85,
        },
    )
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("main_country_code", "DK") in values
    assert ("main_country", "Denmark") in values
    assert all(row["locked"] is True for row in rows)
    assert all(row["evidence_type"] == "csv_tld_strong" for row in rows)


def test_build_geo_prefill_uses_website_url_when_tld_is_missing():
    input_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "website_tld": "",
                "website_url": "https://example.co.uk/about",
                "website_language_code": "",
            }
        ]
    )

    rows = build_geo_prefill(
        input_df,
        {
            "enabled": True,
            "tld_country_confidence": 0.85,
        },
    )
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("main_country_code", "GB") in values
    assert ("main_country", "United Kingdom") in values


def test_build_csv_geo_evidence_uses_language_for_generic_tld():
    input_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "website_tld": "com",
                "website_url": "https://example.com",
                "website_language_code": "da",
            }
        ]
    )

    rows = build_csv_geo_evidence(
        input_df,
        {
            "enabled": True,
            "tld_country_confidence": 0.85,
            "language_country_confidence": 0.55,
        },
    )

    assert rows[0]["evidence_type"] == "csv_language"
    assert rows[0]["field_confidence"] == 0.55
    assert rows[0]["locked"] is False


def test_build_best_evidence_prefers_locked_country_over_official(tmp_path):
    manifest_df = pd.DataFrame(
        [
            {
                "input_row_key": "0",
                "Applications_of_AI_id": "A1",
                "source_col": "website_url",
                "source_type": "official",
                "raw_url": "https://example.com",
                "canonical_url": "https://example.com",
                "domain": "example.com",
                "priority": 1,
            }
        ]
    )
    extracted_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "seed_url": None,
                "canonical_url": None,
                "source_type": "csv",
                "source_col": "website_tld",
                "priority": 0,
                "depth": 0,
                "field_name": "main_country_code",
                "extracted_value": "DK",
                "normalized_value": "DK",
                "evidence_type": "csv_tld_strong",
                "field_confidence": 0.85,
                "evidence_text": "website_tld=dk",
                "locked": True,
                "lock_reason": "country_specific_tld",
            },
            {
                "seed_url": "https://example.com",
                "canonical_url": "https://example.com",
                "source_type": "official",
                "source_col": "website_url",
                "priority": 1,
                "depth": 0,
                "field_name": "main_country_code",
                "extracted_value": "US",
                "normalized_value": "US",
                "evidence_type": "json_ld",
                "field_confidence": 0.75,
                "evidence_text": "addressCountry=US",
                "locked": False,
                "lock_reason": None,
            },
        ]
    )

    manifest_df.to_parquet(tmp_path / "url_manifest.parquet", index=False)
    extracted_df.to_parquet(tmp_path / "crawl_extracted_fields.parquet", index=False)

    result = build_best_evidence({"output_dir": str(tmp_path)})

    assert result.loc[0, "extracted_main_country_code"] == "DK"
