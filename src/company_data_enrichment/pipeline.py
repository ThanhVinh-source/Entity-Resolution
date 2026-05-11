"""
PIPELINE 
--> Central orchestrates the flow of data from raw CSV files to the final enriched result

Core Features:
1. Orchestration: Connects Crawler, Extractor, and Rules Engine into a closed-loop process that starts with raw URLs and ends with enriched company data.
2. Evidence Scoring: Calculates confidence scores based on data sources (Official > CSV > Social) and field-level signals (e.g. TLD-based country inference).
3. Automated Merging: Decides to add/replace data based on predefined thresholds
4. Audit Trail: Creates detailed logs (Action JSON) for all changes to facilitate auditing

PIPELINE FLOW :
STEP 1: build-manifest (in url_manifest) -> Planning (Identify URLs to crawl and perform preliminary TLD/Geo processing)
STEP 2: crawl -> Execution (Bot Crawl4AI collects content) HTML/Markdown)
STEP 3: Extract -> Convert HTML into fields such as Name, Email, Address, Geo...
STEP 4: Merge -> Score evidence and record results in data_enriched.csv
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
    country_name_from_iso,
    infer_country_from_csv_fields,
    infer_strong_country_from_tld,
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
):
    if is_missing(extracted_value):
        return None

    return {
        "Applications_of_AI_id": application_id,
        "seed_url": None,
        "canonical_url": None,
        "source_type": "csv",
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


def run_build_manifest(config):
    ensure_output_dir(config)

    df = read_input_csv(config["input_csv"])
    validation_settings = validation_config(config)
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
        retry_count=config["crawl"].get("retry_count", 0),
        cache_base_dir=config["crawl"].get("cache_base_dir"),
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

    extracted_rows = extract_rows(crawl_rows, validation_settings)
    input_df = read_input_csv(config["input_csv"])
    extracted_rows.extend(build_csv_geo_evidence(input_df, validation_settings))
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
        lambda value: 0.20 if value == "official" else 0.00
    )
    evidence_df["source_rank"] = evidence_df["source_type"].map(
        {
            "official": 0,
            "csv": 1,
            "social": 2,
        }
    ).fillna(3)

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


def run_all(config, limit=None):
    run_build_manifest(config)
    run_crawl(config, limit=limit)
    run_extract(config)
    run_merge(config)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "build-manifest",
            "crawl",
            "extract",
            "merge",
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

    elif args.command == "run-all":
        run_all(config, limit=args.limit)


if __name__ == "__main__":
    main()
