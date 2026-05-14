"""
MODULE: INTEGRATED DATA PIPELINE & ER-READY EXPORTER
--> Task: Coordinates the data journey from raw URLs to normalized datasets (ER-Ready) ready for Entity Resolution.

Core features closely following the source code:
1. Modular Workflow: Connects Crawler, Extractor, and Validation Engine into a single command sequence.
2. Weighted Validation Voting: An intelligent processing layer that resolves geographic data conflicts by weighted voting (TLD vs. JSON-LD vs. Web Content).
3. Rule-Based Decision Engine: Automates decision-making (KEEP/ADD/REPLACE) based on confidence thresholds configured in YAML.
4. ER-Ready Formatting: Dedicated data export tool that automatically selects the best value between the original data and enriched data to load into Databricks.
5. Audit & Quality Traceability: Provides detailed JSON Action logs and a Quality Report for each run.

PIPELINE FLOW:
STEP 1: build-manifest -> PLANNING: Normalize URLs and perform early geographic processing from the domain name (TLD).
STEP 2: crawl -> COLLECT: The Crawl4AI bot retrieves HTML/Markdown content from the queue.
STEP 3: extract -> ANALYSIS: Extract information + Perform country/city voting.
STEP 4: merge -> MERGE: Apply rules to create the data_enriched dataset.
STEP 5: export-er-ready -> Create the optimized file for entity matching (data_er_ready).
"""

import argparse
import json
import math
import os

import pandas as pd
import yaml

from company_data_enrichment.crawler import crawl_records
from company_data_enrichment.extractors import extract_rows
from company_data_enrichment.geo_signals import (
    build_country_signal_votes,
    choose_city_signal,
    choose_country_vote,
    classify_country_verdict,
    country_name_from_iso,
    infer_country_from_csv_fields,
    infer_strong_country_from_tld,
    normalize_country_to_iso,
)
from company_data_enrichment.pandas_io import read_input_csv, read_parquet, write_csv, write_parquet
from company_data_enrichment.quality_report import build_quality_report
from company_data_enrichment.rules import (
    choose_final_value,
    decide_action,
    is_missing,
    normalize_text,
    safe_confidence,
)
from company_data_enrichment.url_manifest import build_crawl_queue, build_url_manifest

EXTRACTED_FIELD_COLUMNS = [
    "Applications_of_AI_id",
    "seed_url",
    "canonical_url",
    "source_type",
    "source_col",
    "priority",
    "depth",
    "field_name",
    "extracted_value",
    "normalized_value",
    "evidence_type",
    "field_confidence",
    "evidence_text",
    "locked",
    "lock_reason",
]

LOCKED_COUNTRY_FIELDS = {"main_country_code", "main_country"}
VALIDATION_ACCEPTED_VERDICTS = {
    "CONFIRMED",
    "CONFLICT_TLD",
    "CONFLICT",
    "MISSING_FILLED",
}
VALIDATION_RESULT_COLUMNS = [
    "Applications_of_AI_id",
    "company_name",
    "db_country",
    "db_country_name",
    "db_city",
    "tld_signal",
    "language_signal",
    "jsonld_country_signal",
    "web_text_country_signal",
    "social_country_signal",
    "voted_country",
    "voted_country_name",
    "vote_confidence",
    "country_match",
    "web_city",
    "city_method",
    "final_method",
    "verdict",
    "recommended_action",
    "all_signals",
    "sources_used",
    "reason",
]


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def output_path(config, name):
    return os.path.join(config["output_dir"], name)


def json_safe(value):
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


def ensure_output_dir(config):
    os.makedirs(config["output_dir"], exist_ok=True)


# note: Read validation-signal settings while defaulting to enabled for backward-compatible enrichment.
def validation_config(config):
    return config.get("validation_signals", {"enabled": True})


# note: Read country-validation settings used for report-style voting.
def country_validation_config(config):
    return config.get("country_validation", {"enabled": True})


# note: Build a locale policy object used by extractor and validation voting.
def locale_policy_config(config):
    country_settings = country_validation_config(config)
    return {
        "weak_locale_evidence_types": country_settings.get(
            "weak_locale_evidence_types",
            [],
        ),
        "ignored_locale_tags": country_settings.get(
            "ignored_locale_tags",
            [],
        ),
        "locale_requires_support": country_settings.get(
            "locale_requires_support",
            False,
        ),
    }


# note: Add locale policy to extractor validation settings so weak locale handling stays config-driven.
def extractor_validation_config(config):
    settings = dict(validation_config(config))
    settings["locale_policy"] = locale_policy_config(config)
    return settings


# note: Build one direct evidence row that can be merged without a crawl manifest URL.
def make_csv_evidence_field(
    application_id,
    field_name,
    extracted_value,
    evidence_type,
    confidence,
    evidence_text,
    source_col,
    locked=False,
    lock_reason=None,
    source_type="csv",
):
    if is_missing(extracted_value):
        return None

    return {
        "Applications_of_AI_id": application_id,
        "seed_url": None,
        "canonical_url": None,
        "source_type": source_type,
        "source_col": source_col,
        "priority": 0,
        "depth": 0,
        "field_name": field_name,
        "extracted_value": extracted_value,
        "normalized_value": normalize_text(extracted_value),
        "evidence_type": evidence_type,
        "field_confidence": float(confidence),
        "evidence_text": evidence_text,
        "locked": bool(locked),
        "lock_reason": lock_reason,
    }


# note: Decide country fields early when a country-specific TLD is available.
def build_geo_prefill(input_df, validation_settings):
    if validation_settings.get("enabled", True) is False:
        return []

    rows = []
    tld_confidence = validation_settings.get("tld_country_confidence", 0.85)

    for _, row in input_df.iterrows():
        country_code, tld, source_col = infer_strong_country_from_tld(
            row.get("website_tld"),
            row.get("website_url"),
        )
        if country_code is None:
            continue

        source_value = row.get(source_col)
        evidence_text = f"{source_col}={source_value}; inferred_tld={tld}"
        application_id = row.get("Applications_of_AI_id")
        country_name = country_name_from_iso(country_code)

        for field_name, extracted_value in [
            ("main_country_code", country_code),
            ("main_country", country_name),
        ]:
            field = make_csv_evidence_field(
                application_id=application_id,
                field_name=field_name,
                extracted_value=extracted_value,
                evidence_type="csv_tld_strong",
                confidence=tld_confidence,
                evidence_text=evidence_text,
                source_col=source_col,
                locked=True,
                lock_reason="country_specific_tld",
            )
            if field:
                rows.append(field)

    return rows


# note: Convert weak CSV metadata such as page language into non-locking country evidence.
def build_csv_geo_evidence(input_df, validation_settings):
    if validation_settings.get("enabled", True) is False:
        return []

    rows = []
    language_confidence = validation_settings.get("language_country_confidence", 0.55)

    for _, row in input_df.iterrows():
        strong_country_code, _, _ = infer_strong_country_from_tld(
            row.get("website_tld"),
            row.get("website_url"),
        )
        if strong_country_code:
            continue

        country_code, method = infer_country_from_csv_fields(
            None,
            row.get("website_language_code"),
        )
        if country_code is None or method != "language":
            continue

        evidence_text = f"website_language_code={row.get('website_language_code')}"
        application_id = row.get("Applications_of_AI_id")
        country_name = country_name_from_iso(country_code)

        for field_name, extracted_value in [
            ("main_country_code", country_code),
            ("main_country", country_name),
        ]:
            field = make_csv_evidence_field(
                application_id=application_id,
                field_name=field_name,
                extracted_value=extracted_value,
                evidence_type="csv_language",
                confidence=language_confidence,
                evidence_text=evidence_text,
                source_col="website_language_code",
            )
            if field:
                rows.append(field)

    return rows


# note: Pull the first country code produced by a selected validation signal family.
def first_signal_country(signals, methods=None, source_type=None):
    method_set = set(methods or [])

    for signal in signals:
        if method_set and signal.get("method") not in method_set:
            continue

        if source_type and signal.get("source") != source_type:
            continue

        return signal.get("country_code")

    return None


# note: Explain the validation verdict in compact text suitable for audit columns.
def validation_reason(row):
    verdict = row.get("verdict")
    voted_country = row.get("voted_country")
    db_country = row.get("db_country")
    final_method = row.get("final_method")
    confidence = row.get("vote_confidence")

    if verdict == "CONFIRMED":
        return f"Validation confirmed original country {db_country} with {final_method} signal."

    if verdict == "CONFLICT_TLD":
        return f"Country-specific TLD voted {voted_country}, conflicting with original country {db_country}."

    if verdict == "CONFLICT":
        return f"Validation voted {voted_country} with confidence {confidence}, conflicting with original country {db_country}."

    if verdict == "MISSING_FILLED":
        return f"Original country was missing; validation filled {voted_country} using {final_method}."

    if verdict == "CONFLICT_UNCERTAIN":
        return f"Country signals disagree with confidence {confidence}; original country kept for review."

    return "Insufficient validation evidence."


# note: Build report-style country validation results from current CSV and crawl evidence, without reading candidate_validation_v2.csv.
def build_company_validation_results(input_df, manifest_df, crawl_extracted_df, config):
    country_settings = country_validation_config(config)
    if country_settings.get("enabled", True) is False:
        return pd.DataFrame(columns=VALIDATION_RESULT_COLUMNS)

    confidence_threshold = float(country_settings.get("vote_confidence_threshold", 0.75))

    if crawl_extracted_df.empty:
        evidence_df = pd.DataFrame()
    else:
        evidence_df = build_evidence_frame(manifest_df, crawl_extracted_df)

    evidence_by_id = {}
    if not evidence_df.empty and "Applications_of_AI_id" in evidence_df.columns:
        for application_id, group in evidence_df.groupby("Applications_of_AI_id"):
            evidence_by_id[str(application_id)] = group.to_dict("records")

    input_unique_df = input_df.drop_duplicates(subset=["Applications_of_AI_id"])
    rows = []
    for _, input_row in input_unique_df.iterrows():
        application_id = input_row.get("Applications_of_AI_id")
        evidence_records = evidence_by_id.get(str(application_id), [])
        signals = build_country_signal_votes(
            website_tld=input_row.get("website_tld"),
            website_url=input_row.get("website_url"),
            website_language_code=input_row.get("website_language_code"),
            evidence_records=evidence_records,
            locale_policy=locale_policy_config(config),
        )
        vote = choose_country_vote(signals)
        db_country = normalize_country_to_iso(input_row.get("main_country_code"))
        verdict, recommended_action = classify_country_verdict(
            db_country,
            vote["voted_country"],
            vote["vote_confidence"],
            vote["final_method"],
            confidence_threshold=confidence_threshold,
        )
        web_city, city_method, _ = choose_city_signal(evidence_records)
        sources_used = sorted(
            {
                f"{signal.get('method')}:{signal.get('source')}"
                for signal in signals
            }
        )

        row = {
            "Applications_of_AI_id": application_id,
            "company_name": input_row.get("company_name"),
            "db_country": db_country,
            "db_country_name": country_name_from_iso(db_country),
            "db_city": input_row.get("main_city"),
            "tld_signal": first_signal_country(signals, ["tld"]),
            "language_signal": first_signal_country(signals, ["language"]),
            "jsonld_country_signal": first_signal_country(signals, ["json_ld"]),
            "web_text_country_signal": first_signal_country(
                signals,
                ["page_text_country", "meta_geo", "html_lang"],
            ),
            "social_country_signal": first_signal_country(
                signals,
                ["page_text_country", "meta_geo", "html_lang", "json_ld"],
                source_type="social",
            ),
            "voted_country": vote["voted_country"],
            "voted_country_name": country_name_from_iso(vote["voted_country"]),
            "vote_confidence": vote["vote_confidence"],
            "country_match": vote["voted_country"] == db_country
            if vote["voted_country"] and db_country
            else None,
            "web_city": web_city,
            "city_method": city_method,
            "final_method": vote["final_method"],
            "verdict": verdict,
            "recommended_action": recommended_action,
            "all_signals": json.dumps(vote["all_signals"], ensure_ascii=False),
            "sources_used": " | ".join(sources_used),
            "reason": None,
        }
        row["reason"] = validation_reason(row)
        rows.append(row)

    return pd.DataFrame(rows, columns=VALIDATION_RESULT_COLUMNS)


# note: Convert validation report rows into mergeable evidence for country and city fields.
def build_validation_evidence_rows(validation_results_df, config):
    country_settings = country_validation_config(config)
    if country_settings.get("enabled", True) is False or validation_results_df.empty:
        return []

    confidence_threshold = float(country_settings.get("vote_confidence_threshold", 0.75))
    city_confidence = float(
        validation_config(config).get("city_pattern_confidence", 0.60)
    )
    rows = []

    for _, row in validation_results_df.iterrows():
        verdict = row.get("verdict")
        voted_country = row.get("voted_country")
        vote_confidence = safe_confidence(row.get("vote_confidence"))

        if (
            verdict in VALIDATION_ACCEPTED_VERDICTS
            and vote_confidence >= confidence_threshold
            and not is_missing(voted_country)
        ):
            country_name = country_name_from_iso(voted_country)
            evidence_text = row.get("reason")

            for field_name, extracted_value in [
                ("main_country_code", voted_country),
                ("main_country", country_name),
            ]:
                field = make_csv_evidence_field(
                    application_id=row.get("Applications_of_AI_id"),
                    field_name=field_name,
                    extracted_value=extracted_value,
                    evidence_type="validation_vote_country",
                    confidence=vote_confidence,
                    evidence_text=evidence_text,
                    source_col=row.get("final_method") or "validation_vote",
                    locked=True,
                    lock_reason=verdict,
                    source_type="validation",
                )
                if field:
                    rows.append(field)

        if not is_missing(row.get("web_city")):
            field = make_csv_evidence_field(
                application_id=row.get("Applications_of_AI_id"),
                field_name="main_city",
                extracted_value=row.get("web_city"),
                evidence_type="validation_vote_city",
                confidence=city_confidence,
                evidence_text=row.get("reason"),
                source_col=row.get("city_method") or "validation_city",
                source_type="validation",
            )
            if field:
                rows.append(field)

    return rows


def run_build_manifest(config):
    ensure_output_dir(config)

    df = read_input_csv(config["input_csv"])
    validation_settings = extractor_validation_config(config)
    manifest_df = build_url_manifest(df, config["url_columns"])
    queue_df = build_crawl_queue(manifest_df)
    geo_prefill_df = pd.DataFrame(
        build_geo_prefill(df, validation_settings),
        columns=EXTRACTED_FIELD_COLUMNS,
    )

    write_parquet(manifest_df, output_path(config, "url_manifest.parquet"))
    write_parquet(queue_df, output_path(config, "crawl_queue.parquet"))
    write_parquet(geo_prefill_df, output_path(config, "geo_prefill.parquet"))

    print("Built URL manifest")
    print("Manifest rows:", len(manifest_df))
    print("Crawl queue rows:", len(queue_df))
    print("Geo prefill rows:", len(geo_prefill_df))


def print_crawl_results_summary(results_df):
    if results_df.empty:
        print("Crawl results summary: no rows.")
        return

    for column_name in ["source_type", "success"]:
        if column_name not in results_df.columns:
            print(f"Crawl results summary: missing {column_name} column.")
            continue

        print(results_df[column_name].value_counts(dropna=False))


def run_crawl(config, limit=None):
    ensure_output_dir(config)

    queue_df = read_parquet(output_path(config, "crawl_queue.parquet"))

    if limit:
        queue_df = queue_df.head(int(limit))

    records = queue_df.to_dict("records")

    results = crawl_records(
        records=records,
        max_concurrency=config["crawl"]["max_concurrency"],
        timeout_ms=config["crawl"]["timeout_ms"],
        seed_timeout_ms=config["crawl"].get("seed_timeout_ms"),
        retry_count=config["crawl"].get("retry_count", 0),
        cache_base_dir=config["crawl"].get("cache_base_dir"),
        browser_recycle_every=config["crawl"].get("browser_recycle_every"),
        deep_crawl_enabled=config["crawl"].get("deep_crawl_enabled", False),
        deep_crawl_max_depth=config["crawl"].get("deep_crawl_max_depth", 1),
        deep_crawl_max_pages=config["crawl"].get("deep_crawl_max_pages", 5),
        deep_crawl_include_external=config["crawl"].get(
            "deep_crawl_include_external",
            False,
        ),
    )

    results_df = pd.DataFrame(results)
    write_parquet(results_df, output_path(config, "crawl_results_raw.parquet"))

    print("Crawled URLs:", len(results))
    print_crawl_results_summary(results_df)


def run_extract(config):
    ensure_output_dir(config)

    crawl_df = read_parquet(output_path(config, "crawl_results_raw.parquet"))
    crawl_rows = crawl_df.to_dict("records")
    validation_settings = validation_config(config)

    crawl_extracted_rows = extract_rows(crawl_rows, validation_settings)
    input_df = read_input_csv(config["input_csv"])
    manifest_df = read_parquet(output_path(config, "url_manifest.parquet"))
    crawl_extracted_df = pd.DataFrame(
        crawl_extracted_rows,
        columns=EXTRACTED_FIELD_COLUMNS,
    )
    validation_results_df = build_company_validation_results(
        input_df,
        manifest_df,
        crawl_extracted_df,
        config,
    )
    validation_evidence_rows = build_validation_evidence_rows(
        validation_results_df,
        config,
    )

    write_parquet(
        validation_results_df,
        output_path(config, "company_validation_results.parquet"),
    )
    write_csv(
        validation_results_df,
        output_path(config, "company_validation_results.csv"),
    )

    extracted_rows = list(crawl_extracted_rows)
    extracted_rows.extend(build_csv_geo_evidence(input_df, validation_settings))
    extracted_rows.extend(validation_evidence_rows)
    geo_prefill_path = output_path(config, "geo_prefill.parquet")

    if validation_settings.get("enabled", True) is False:
        geo_prefill_df = pd.DataFrame(columns=EXTRACTED_FIELD_COLUMNS)
    elif os.path.exists(geo_prefill_path):
        geo_prefill_df = read_parquet(geo_prefill_path)
    else:
        geo_prefill_df = pd.DataFrame(
            build_geo_prefill(input_df, validation_settings),
            columns=EXTRACTED_FIELD_COLUMNS,
        )

    extracted_df = pd.DataFrame(extracted_rows, columns=EXTRACTED_FIELD_COLUMNS)
    extracted_df = pd.concat(
        [extracted_df, geo_prefill_df],
        ignore_index=True,
        sort=False,
    )
    extracted_df = extracted_df.reindex(columns=EXTRACTED_FIELD_COLUMNS)
    write_parquet(extracted_df, output_path(config, "crawl_extracted_fields.parquet"))

    print("Extracted field rows:", len(extracted_df))
    print("Validation result rows:", len(validation_results_df))


# note: Attach crawl-derived evidence to company IDs and keep direct CSV evidence as-is.
def build_evidence_frame(manifest_df, extracted_df):
    extracted_df = extracted_df.copy()

    if "Applications_of_AI_id" not in extracted_df.columns:
        extracted_df["Applications_of_AI_id"] = None

    if "seed_url" not in extracted_df.columns:
        extracted_df["seed_url"] = extracted_df["canonical_url"]

    direct_mask = extracted_df["Applications_of_AI_id"].notna() & (
        extracted_df["Applications_of_AI_id"].astype(str).str.strip() != ""
    )
    direct_df = extracted_df[direct_mask].rename(columns={"canonical_url": "page_url"})
    crawl_df = extracted_df[~direct_mask].copy()

    if crawl_df.empty:
        return direct_df

    crawl_df = crawl_df.rename(columns={"canonical_url": "page_url"})
    crawl_df = crawl_df.drop(
        columns=["Applications_of_AI_id", "source_type", "source_col", "priority"],
        errors="ignore",
    )

    joined_df = manifest_df.merge(
        crawl_df,
        left_on="canonical_url",
        right_on="seed_url",
        how="inner",
        suffixes=("_seed", "_extracted"),
    )

    if direct_df.empty:
        return joined_df

    return pd.concat([joined_df, direct_df], ignore_index=True, sort=False)


# note: Normalize truthy values from older/newer parquet files into booleans for locked evidence.
def bool_value(value):
    if isinstance(value, bool):
        return value

    if value is None:
        return False

    if isinstance(value, float) and math.isnan(value):
        return False

    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def build_best_evidence(config):
    manifest_df = read_parquet(output_path(config, "url_manifest.parquet"))
    extracted_df = read_parquet(output_path(config, "crawl_extracted_fields.parquet"))

    if extracted_df.empty:
        return pd.DataFrame(columns=["Applications_of_AI_id", "evidence_json"])

    evidence_df = build_evidence_frame(manifest_df, extracted_df)

    if evidence_df.empty:
        return pd.DataFrame(columns=["Applications_of_AI_id", "evidence_json"])

    if "locked" not in evidence_df.columns:
        evidence_df["locked"] = False

    if "lock_reason" not in evidence_df.columns:
        evidence_df["lock_reason"] = None

    evidence_df["locked"] = evidence_df["locked"].apply(bool_value)
    evidence_df["field_confidence"] = pd.to_numeric(
        evidence_df["field_confidence"],
        errors="coerce",
    ).fillna(0.0)
    evidence_df["source_bonus"] = evidence_df["source_type"].apply(
        lambda value: 0.20 if value in {"official", "validation"} else 0.00
    )
    evidence_df["source_rank"] = evidence_df["source_type"].map(
        {
            "validation": 0,
            "official": 1,
            "csv": 2,
            "social": 3,
        }
    ).fillna(4)

    if "depth" not in evidence_df.columns:
        evidence_df["depth"] = 0

    evidence_df["depth"] = pd.to_numeric(
        evidence_df["depth"],
        errors="coerce",
    ).fillna(0)
    evidence_df["final_confidence"] = (
        evidence_df["field_confidence"] + evidence_df["source_bonus"]
    ).clip(upper=1.0)
    evidence_df["locked_country_rank"] = evidence_df.apply(
        lambda row: 0
        if row.get("field_name") in LOCKED_COUNTRY_FIELDS and row.get("locked")
        else 1,
        axis=1,
    )

    best_df = (
        evidence_df.sort_values(
            [
                "Applications_of_AI_id",
                "field_name",
                "locked_country_rank",
                "final_confidence",
                "source_rank",
                "depth",
                "priority",
            ],
            ascending=[True, True, True, False, True, True, True],
        )
        .drop_duplicates(subset=["Applications_of_AI_id", "field_name"], keep="first")
        .copy()
    )

    values_df = (
        best_df.pivot(
            index="Applications_of_AI_id",
            columns="field_name",
            values="extracted_value",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    values_df = values_df.rename(
        columns={
            column: "extracted_" + column
            for column in values_df.columns
            if column != "Applications_of_AI_id"
        }
    )

    confidence_df = (
        best_df.pivot(
            index="Applications_of_AI_id",
            columns="field_name",
            values="final_confidence",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    confidence_df = confidence_df.rename(
        columns={
            column: "extracted_" + column + "_confidence"
            for column in confidence_df.columns
            if column != "Applications_of_AI_id"
        }
    )

    source_rows = []

    for application_id, group in best_df.groupby("Applications_of_AI_id"):
        source_rows.append(
            {
                "Applications_of_AI_id": application_id,
                "evidence_json": build_evidence_json(group),
            }
        )

    source_df = pd.DataFrame(source_rows)

    wide_df = values_df.merge(confidence_df, on="Applications_of_AI_id", how="left")
    wide_df = wide_df.merge(source_df, on="Applications_of_AI_id", how="left")
    return wide_df


def build_evidence_json(group):
    items = []

    for _, row in group.iterrows():
        items.append(
            {
                "field_name": json_safe(row.get("field_name")),
                "extracted_value": json_safe(row.get("extracted_value")),
                "final_confidence": json_safe(row.get("final_confidence")),
                "seed_url": json_safe(row.get("seed_url")),
                "page_url": json_safe(row.get("page_url")),
                "source_type": json_safe(row.get("source_type")),
                "source_col": json_safe(row.get("source_col")),
                "depth": json_safe(row.get("depth")),
                "evidence_type": json_safe(row.get("evidence_type")),
                "evidence_text": json_safe(row.get("evidence_text")),
                "locked": json_safe(row.get("locked")),
                "lock_reason": json_safe(row.get("lock_reason")),
            }
        )

    return json.dumps(items, ensure_ascii=False)


def build_actions_json(row, target_fields):
    items = []

    for field_name in target_fields:
        extracted_col = "extracted_" + field_name
        confidence_col = extracted_col + "_confidence"
        final_col = "final_" + field_name
        action_col = field_name + "_action"

        items.append(
            {
                "field_name": field_name,
                "action": json_safe(row.get(action_col)),
                "original_value": json_safe(row.get(field_name)),
                "extracted_value": json_safe(row.get(extracted_col)),
                "final_value": json_safe(row.get(final_col)),
                "confidence": json_safe(row.get(confidence_col)),
            }
        )

    return json.dumps(items, ensure_ascii=False)


def run_merge(config):
    ensure_output_dir(config)

    original_df = read_input_csv(config["input_csv"])
    evidence_wide_df = build_best_evidence(config)

    enriched_df = original_df.merge(
        evidence_wide_df,
        on="Applications_of_AI_id",
        how="left",
    )

    add_threshold = config["rules"]["add_threshold"]
    replace_threshold = config["rules"]["replace_threshold"]

    for field_name in config["target_fields"]:
        extracted_col = "extracted_" + field_name
        confidence_col = extracted_col + "_confidence"
        final_col = "final_" + field_name
        action_col = field_name + "_action"

        if extracted_col not in enriched_df.columns:
            enriched_df[extracted_col] = None

        if confidence_col not in enriched_df.columns:
            enriched_df[confidence_col] = 0.0

        enriched_df[confidence_col] = enriched_df[confidence_col].apply(safe_confidence)

        enriched_df[action_col] = enriched_df.apply(
            lambda row: decide_action(
                original_value=row.get(field_name),
                extracted_value=row.get(extracted_col),
                confidence=row.get(confidence_col),
                add_threshold=add_threshold,
                replace_threshold=replace_threshold,
            ),
            axis=1,
        )

        enriched_df[final_col] = enriched_df.apply(
            lambda row: choose_final_value(
                original_value=row.get(field_name),
                extracted_value=row.get(extracted_col),
                confidence=row.get(confidence_col),
                add_threshold=add_threshold,
                replace_threshold=replace_threshold,
            ),
            axis=1,
        )

    if "evidence_json" not in enriched_df.columns:
        enriched_df["evidence_json"] = None

    enriched_df["evidence_json"] = enriched_df["evidence_json"].apply(
        lambda value: None if is_missing(value) else value
    )
    enriched_df["enrichment_actions_json"] = enriched_df.apply(
        lambda row: build_actions_json(row, config["target_fields"]),
        axis=1,
    )

    write_parquet(enriched_df, output_path(config, "data_enriched.parquet"))
    write_csv(enriched_df, output_path(config, "data_enriched.csv"))

    report_df = build_quality_report(enriched_df, config["target_fields"])

    if report_df is not None:
        write_parquet(report_df, output_path(config, "quality_report.parquet"))
        print(report_df.to_string(index=False))

    print("Built enriched company dataset")


# note: Choose an ER-ready value with a field-specific confidence threshold.
def choose_er_value(row, original_col, final_col, confidence_col, threshold):
    original_value = row.get(original_col)
    final_value = row.get(final_col)
    confidence = safe_confidence(row.get(confidence_col))

    if not is_missing(final_value) and confidence >= threshold:
        return final_value, "final"

    if not is_missing(original_value):
        return original_value, "original"

    if not is_missing(final_value):
        return final_value, "final_low_confidence"

    return None, "missing"


# note: Choose enriched value only when the original field is missing.
def choose_er_add_only_value(row, original_col, final_col, confidence_col, threshold):
    original_value = row.get(original_col)
    final_value = row.get(final_col)
    confidence = safe_confidence(row.get(confidence_col))

    if not is_missing(original_value):
        return original_value, "original"

    if not is_missing(final_value) and confidence >= threshold:
        return final_value, "final_added"

    if not is_missing(final_value):
        return final_value, "final_low_confidence"

    return None, "missing"


# note: Choose original company name unless it is missing, keeping crawler names out of ER by default.
def choose_company_name_for_er(row):
    original_value = row.get("company_name")
    final_value = row.get("final_company_name")

    if not is_missing(original_value):
        return original_value, "original"

    if not is_missing(final_value):
        return final_value, "final"

    return None, "missing"


# note: Choose country fields using validation-aware final country code and canonical country names.
def choose_country_for_er(row, validation_row=None):
    original_code = normalize_country_to_iso(row.get("main_country_code"))
    final_code = normalize_country_to_iso(row.get("final_main_country_code"))
    selected_code = final_code or original_code
    selected_name = (
        country_name_from_iso(selected_code)
        or row.get("final_main_country")
        or row.get("main_country")
    )
    source = "original"

    if validation_row is not None:
        action = validation_row.get("recommended_action")
        voted_country = validation_row.get("voted_country")
        if action in {"CORRECT_COUNTRY", "FILL_COUNTRY", "KEEP"} and voted_country == selected_code:
            source = "validation"
        elif final_code and final_code != original_code:
            source = "enrichment"
    elif final_code and final_code != original_code:
        source = "enrichment"

    return selected_code, selected_name, source


# note: Add a column from the source dataframe when present, otherwise fill with blanks.
def copy_optional_column(output_df, input_df, column_name):
    if column_name in input_df.columns:
        output_df[column_name] = input_df[column_name]
    else:
        output_df[column_name] = ""


# note: Build the compact Databricks upload file from full enrichment output.
def build_er_ready_frame(enriched_df, validation_results_df, config):
    er_config = config.get("er_ready", {})
    city_threshold = float(er_config.get("city_confidence_threshold", 0.80))
    email_threshold = float(er_config.get("email_confidence_threshold", 0.85))
    description_threshold = float(
        er_config.get("description_confidence_threshold", 0.75)
    )
    year_founded_threshold = float(
        er_config.get("year_founded_confidence_threshold", 0.80)
    )
    employee_count_threshold = float(
        er_config.get("employee_count_confidence_threshold", 0.75)
    )

    validation_by_id = {}
    if not validation_results_df.empty:
        validation_by_id = {
            str(row.get("Applications_of_AI_id")): row
            for _, row in validation_results_df.iterrows()
        }

    output_df = pd.DataFrame()
    for column_name in [
        "input_row_key",
        "Applications_of_AI_id",
        "input_company_name",
        "input_main_country_code",
        "input_main_country",
        "input_main_region",
        "input_main_city",
        "input_main_postcode",
        "input_main_street",
        "input_main_street_number",
    ]:
        copy_optional_column(output_df, enriched_df, column_name)

    company_names = enriched_df.apply(choose_company_name_for_er, axis=1)
    output_df["company_name"] = [value for value, _ in company_names]
    output_df["company_name_source"] = [source for _, source in company_names]

    for column_name in ["company_legal_names", "company_commercial_names"]:
        copy_optional_column(output_df, enriched_df, column_name)

    country_values = []
    for _, row in enriched_df.iterrows():
        validation_row = validation_by_id.get(str(row.get("Applications_of_AI_id")))
        country_values.append(choose_country_for_er(row, validation_row))

    output_df["main_country_code"] = [value[0] for value in country_values]
    output_df["main_country"] = [value[1] for value in country_values]
    output_df["country_source"] = [value[2] for value in country_values]

    for column_name in ["main_region", "main_postcode", "main_street"]:
        copy_optional_column(output_df, enriched_df, column_name)

    city_values = enriched_df.apply(
        lambda row: choose_er_value(
            row,
            "main_city",
            "final_main_city",
            "extracted_main_city_confidence",
            city_threshold,
        ),
        axis=1,
    )
    output_df["main_city"] = [value for value, _ in city_values]
    output_df["city_source"] = [source for _, source in city_values]

    email_values = enriched_df.apply(
        lambda row: choose_er_value(
            row,
            "primary_email",
            "final_primary_email",
            "extracted_primary_email_confidence",
            email_threshold,
        ),
        axis=1,
    )
    output_df["primary_email"] = [value for value, _ in email_values]
    output_df["email_source"] = [source for _, source in email_values]

    copy_optional_column(output_df, enriched_df, "primary_phone")

    description_values = enriched_df.apply(
        lambda row: choose_er_value(
            row,
            "short_description",
            "final_short_description",
            "extracted_short_description_confidence",
            description_threshold,
        ),
        axis=1,
    )
    output_df["short_description"] = [value for value, _ in description_values]
    output_df["description_source"] = [source for _, source in description_values]

    year_founded_values = enriched_df.apply(
        lambda row: choose_er_add_only_value(
            row,
            "year_founded",
            "final_year_founded",
            "extracted_year_founded_confidence",
            year_founded_threshold,
        ),
        axis=1,
    )
    output_df["year_founded"] = [value for value, _ in year_founded_values]
    output_df["year_founded_source"] = [source for _, source in year_founded_values]

    employee_count_values = enriched_df.apply(
        lambda row: choose_er_add_only_value(
            row,
            "employee_count",
            "final_employee_count",
            "extracted_employee_count_confidence",
            employee_count_threshold,
        ),
        axis=1,
    )
    output_df["employee_count"] = [value for value, _ in employee_count_values]
    output_df["employee_count_source"] = [source for _, source in employee_count_values]

    for column_name in [
        "generated_description",
        "generated_business_tags",
        "long_description",
        "business_tags",
        "main_business_category",
        "main_industry",
        "main_sector",
        "company_type",
        "revenue",
        "website_url",
        "website_domain",
        "website_tld",
        "website_language_code",
        "linkedin_url",
        "facebook_url",
        "twitter_url",
        "instagram_url",
        "youtube_url",
        "naics_2022_primary_code",
        "naics_2022_primary_label",
        "naics_2022_secondary_codes",
        "naics_2022_secondary_labels",
        "sic_codes",
        "sic_labels",
        "nace_rev2_codes",
        "nace_rev2_labels",
        "isic_v4_codes",
        "isic_v4_labels",
    ]:
        if column_name in enriched_df.columns:
            output_df[column_name] = enriched_df[column_name]

    if validation_by_id:
        validation_slim = validation_results_df.drop_duplicates(
            subset=["Applications_of_AI_id"]
        )[
            [
                "Applications_of_AI_id",
                "verdict",
                "vote_confidence",
                "final_method",
                "recommended_action",
            ]
        ].rename(
            columns={
                "verdict": "country_validation_verdict",
                "vote_confidence": "country_validation_confidence",
                "final_method": "country_validation_method",
                "recommended_action": "country_validation_action",
            }
        )
        output_df = output_df.merge(
            validation_slim,
            on="Applications_of_AI_id",
            how="left",
        )
    else:
        output_df["country_validation_verdict"] = ""
        output_df["country_validation_confidence"] = ""
        output_df["country_validation_method"] = ""
        output_df["country_validation_action"] = ""

    output_df["source_enrichment_file"] = "data_enriched.csv"
    return output_df


def run_export_er_ready(config):
    ensure_output_dir(config)

    enriched_path = output_path(config, "data_enriched.parquet")
    validation_path = output_path(config, "company_validation_results.parquet")
    enriched_df = read_parquet(enriched_path)

    if os.path.exists(validation_path):
        validation_results_df = read_parquet(validation_path)
    else:
        validation_results_df = pd.DataFrame(columns=VALIDATION_RESULT_COLUMNS)

    er_ready_df = build_er_ready_frame(enriched_df, validation_results_df, config)
    write_parquet(er_ready_df, output_path(config, "data_er_ready.parquet"))
    write_csv(er_ready_df, output_path(config, "data_er_ready.csv"))

    print("Built ER-ready dataset")
    print("ER-ready rows:", len(er_ready_df))
    print("ER-ready columns:", len(er_ready_df.columns))


def run_all(config, limit=None):
    run_build_manifest(config)
    run_crawl(config, limit=limit)
    run_extract(config)
    run_merge(config)
    run_export_er_ready(config)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "build-manifest",
            "crawl",
            "extract",
            "merge",
            "export-er-ready",
            "run-all",
        ],
    )
    parser.add_argument(
        "--config",
        default="config/company_data_enrichment.yaml",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )

    args = parser.parse_args()
    config = load_config(args.config)

    if args.command == "build-manifest":
        run_build_manifest(config)

    elif args.command == "crawl":
        run_crawl(config, limit=args.limit)

    elif args.command == "extract":
        run_extract(config)

    elif args.command == "merge":
        run_merge(config)

    elif args.command == "export-er-ready":
        run_export_er_ready(config)

    elif args.command == "run-all":
        run_all(config, limit=args.limit)


if __name__ == "__main__":
    main()
