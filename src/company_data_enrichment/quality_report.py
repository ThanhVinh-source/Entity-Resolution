import pandas as pd

# QUALITY REPORTING
# Generates a quality report based on the actions taken for each target field during the enrichment process. 
def build_quality_report(enriched_df, target_fields):
    reports = []

    for field_name in target_fields:
        column_name = field_name + "_action"

        if column_name not in enriched_df.columns:
            continue

        counts = (
            enriched_df[column_name]
            .fillna("KEEP")
            .value_counts(dropna=False)
            .reset_index()
        )
        counts.columns = ["action", "count"]
        counts["field_name"] = field_name
        reports.append(counts[["field_name", "action", "count"]])

    if len(reports) == 0:
        return None

    report_df = pd.concat(reports, ignore_index=True)
    report_df = report_df.sort_values(["field_name", "action"]).reset_index(drop=True)
    return report_df
