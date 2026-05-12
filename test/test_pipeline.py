import json

import pandas as pd

from company_data_enrichment.pipeline import (
    build_best_evidence,
    build_company_validation_results,
    build_csv_geo_evidence,
    build_er_ready_frame,
    build_geo_prefill,
    build_validation_evidence_rows,
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


def test_company_validation_results_flags_tld_conflict():
    input_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "company_name": "CrowdStrike Inc s.r.o.",
                "main_country_code": "US",
                "main_city": "",
                "website_tld": "sk",
                "website_url": "https://example.sk",
                "website_language_code": "",
            }
        ]
    )

    result = build_company_validation_results(
        input_df,
        pd.DataFrame(),
        pd.DataFrame(),
        {"country_validation": {"enabled": True, "vote_confidence_threshold": 0.75}},
    )

    assert result.loc[0, "voted_country"] == "SK"
    assert result.loc[0, "verdict"] == "CONFLICT_TLD"
    assert result.loc[0, "recommended_action"] == "CORRECT_COUNTRY"


def test_validation_evidence_rows_create_locked_country_evidence():
    validation_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "voted_country": "DK",
                "vote_confidence": 0.75,
                "verdict": "CONFLICT",
                "recommended_action": "CORRECT_COUNTRY",
                "final_method": "page_text_country",
                "web_city": "",
                "reason": "Validation voted DK.",
            }
        ]
    )

    rows = build_validation_evidence_rows(
        validation_df,
        {
            "country_validation": {
                "enabled": True,
                "vote_confidence_threshold": 0.75,
            },
            "validation_signals": {
                "city_pattern_confidence": 0.60,
            },
        },
    )

    country_rows = [row for row in rows if row["field_name"] == "main_country_code"]

    assert country_rows[0]["extracted_value"] == "DK"
    assert country_rows[0]["source_type"] == "validation"
    assert country_rows[0]["locked"] is True
    assert country_rows[0]["lock_reason"] == "CONFLICT"


def test_build_best_evidence_prefers_validation_country_over_official(tmp_path):
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
                "source_type": "validation",
                "source_col": "tld",
                "priority": 0,
                "depth": 0,
                "field_name": "main_country_code",
                "extracted_value": "DK",
                "normalized_value": "DK",
                "evidence_type": "validation_vote_country",
                "field_confidence": 0.75,
                "evidence_text": "Validation voted DK",
                "locked": True,
                "lock_reason": "CONFLICT_TLD",
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
                "evidence_type": "html_lang",
                "field_confidence": 0.55,
                "evidence_text": "en-US",
                "locked": False,
                "lock_reason": None,
            },
        ]
    )

    manifest_df.to_parquet(tmp_path / "url_manifest.parquet", index=False)
    extracted_df.to_parquet(tmp_path / "crawl_extracted_fields.parquet", index=False)

    result = build_best_evidence({"output_dir": str(tmp_path)})

    assert result.loc[0, "extracted_main_country_code"] == "DK"
    assert result.loc[0, "extracted_main_country_code_confidence"] == 0.95


def test_build_er_ready_frame_keeps_original_name_and_uses_validated_country():
    enriched_df = pd.DataFrame(
        [
            {
                "input_row_key": "0",
                "Applications_of_AI_id": "A1",
                "company_name": "Original Company",
                "final_company_name": "Wrong Crawled Name",
                "company_legal_names": "Original Company ApS",
                "company_commercial_names": "",
                "main_country_code": "US",
                "final_main_country_code": "DK",
                "final_main_country": "Denmark",
                "main_city": "Old City",
                "final_main_city": "Copenhagen",
                "extracted_main_city_confidence": 0.85,
                "primary_email": "old@example.com",
                "final_primary_email": "new@example.com",
                "extracted_primary_email_confidence": 0.90,
                "primary_phone": "+4512345678",
                "short_description": "Old description",
                "final_short_description": "New description",
                "extracted_short_description_confidence": 0.80,
                "year_founded": "1999",
                "final_year_founded": "2001",
                "extracted_year_founded_confidence": 0.85,
                "employee_count": "50",
                "final_employee_count": "120",
                "extracted_employee_count_confidence": 0.80,
                "website_url": "https://example.dk",
            }
        ]
    )
    validation_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "verdict": "CONFLICT_TLD",
                "vote_confidence": 1.0,
                "final_method": "tld",
                "recommended_action": "CORRECT_COUNTRY",
                "voted_country": "DK",
            }
        ]
    )

    result = build_er_ready_frame(
        enriched_df,
        validation_df,
        {
            "er_ready": {
                "city_confidence_threshold": 0.80,
                "email_confidence_threshold": 0.85,
                "description_confidence_threshold": 0.75,
                "year_founded_confidence_threshold": 0.80,
                "employee_count_confidence_threshold": 0.75,
            }
        },
    )

    assert result.loc[0, "company_name"] == "Original Company"
    assert result.loc[0, "company_name_source"] == "original"
    assert result.loc[0, "main_country_code"] == "DK"
    assert result.loc[0, "country_source"] == "validation"
    assert result.loc[0, "main_city"] == "Copenhagen"
    assert result.loc[0, "primary_email"] == "new@example.com"
    assert result.loc[0, "short_description"] == "New description"
    assert result.loc[0, "year_founded"] == "1999"
    assert result.loc[0, "year_founded_source"] == "original"
    assert result.loc[0, "employee_count"] == "50"
    assert result.loc[0, "employee_count_source"] == "original"


def test_build_er_ready_frame_adds_year_and_employee_count_when_original_missing():
    enriched_df = pd.DataFrame(
        [
            {
                "Applications_of_AI_id": "A1",
                "company_name": "Original Company",
                "main_country_code": "DK",
                "final_main_country_code": "DK",
                "year_founded": "",
                "final_year_founded": "2001",
                "extracted_year_founded_confidence": 0.85,
                "employee_count": "",
                "final_employee_count": "120",
                "extracted_employee_count_confidence": 0.80,
            }
        ]
    )

    result = build_er_ready_frame(
        enriched_df,
        pd.DataFrame(),
        {
            "er_ready": {
                "year_founded_confidence_threshold": 0.80,
                "employee_count_confidence_threshold": 0.75,
            }
        },
    )

    assert result.loc[0, "year_founded"] == "2001"
    assert result.loc[0, "year_founded_source"] == "final_added"
    assert result.loc[0, "employee_count"] == "120"
    assert result.loc[0, "employee_count_source"] == "final_added"
