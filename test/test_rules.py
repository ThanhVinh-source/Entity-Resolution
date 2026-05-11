from company_data_enrichment.rules import decide_action, normalize_text


def test_normalize_text():
    assert normalize_text("Acme, GmbH!") == "ACME GMBH"


def test_add_when_original_missing():
    action = decide_action(
        original_value="",
        extracted_value="Acme GmbH",
        confidence=0.70,
        add_threshold=0.60,
        replace_threshold=0.80,
    )

    assert action == "ADD"


def test_replace_when_confidence_high():
    action = decide_action(
        original_value="Old Name Ltd",
        extracted_value="New Name Ltd",
        confidence=0.90,
        add_threshold=0.60,
        replace_threshold=0.80,
    )

    assert action == "REPLACE"


def test_conflict_review_when_confidence_low():
    action = decide_action(
        original_value="Old Name Ltd",
        extracted_value="New Name Ltd",
        confidence=0.70,
        add_threshold=0.60,
        replace_threshold=0.80,
    )

    assert action == "CONFLICT_REVIEW"


def test_keep_when_values_same():
    action = decide_action(
        original_value="Acme GmbH",
        extracted_value="ACME GmbH",
        confidence=0.90,
        add_threshold=0.60,
        replace_threshold=0.80,
    )

    assert action == "KEEP"
