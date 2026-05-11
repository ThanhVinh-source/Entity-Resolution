"""
MODULE: DATA MERGING and DECISION RULES
--> Makes the final decision on integrating extracted data into the original dataset.

Core Features:
1. Normalization: Normalizes text (capitalization, removal of extraneous characters) for fair data comparison.
2. Threshold-Based Logic: Uses confidence thresholds (Add/Replace Thresholds) to automate decision-making.
3. Action Classification: Clearly categorizes actions:
- KEEP: Keeps the old data.
- ADD: Fills in the blank when sufficient confidence is reached.
- REPLACE: Overwrites the old data with new, more accurate data.
- CONFLICT_REVIEW: Marks conflicting cases requiring human review.
4. Value Selection: A function that selects the final value for the Gold data layer.
"""

# RULE:
# This module defines the rules for deciding whether to keep, add, replace, or mark for review the extracted company data based on the original value, the extracted value, and the confidence score. 
# It includes functions to check for missing values, normalize text for comparison, and safely handle confidence scores.

import math
import re


def is_missing(value):
    if value is None:
        return True

    if isinstance(value, float) and math.isnan(value):
        return True

    return str(value).strip() == ""


def normalize_text(value):
    if is_missing(value):
        return ""

    value = str(value).upper()
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def safe_confidence(value):
    if is_missing(value):
        return 0.0

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def decide_action(original_value, extracted_value, confidence, add_threshold, replace_threshold):
    confidence = safe_confidence(confidence)
    original_missing = is_missing(original_value)
    extracted_missing = is_missing(extracted_value)

    if extracted_missing:
        return "KEEP"

    if original_missing and confidence >= add_threshold:
        return "ADD"

    if normalize_text(original_value) == normalize_text(extracted_value):
        return "KEEP"

    if confidence >= replace_threshold:
        return "REPLACE"

    return "CONFLICT_REVIEW"


def choose_final_value(original_value, extracted_value, confidence, add_threshold, replace_threshold):
    action = decide_action(
        original_value=original_value,
        extracted_value=extracted_value,
        confidence=confidence,
        add_threshold=add_threshold,
        replace_threshold=replace_threshold,
    )

    if action in ["ADD", "REPLACE"]:
        return extracted_value

    return original_value
