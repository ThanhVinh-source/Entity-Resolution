from company_data_enrichment.extractors import extract_from_html


def test_extract_jsonld_organization_name():
    html = """
    <html>
      <head>
        <script type="application/ld+json">
        {
          "@type": "Organization",
          "name": "Acme Manufacturing",
          "legalName": "Acme Manufacturing GmbH",
          "email": "info@acme.com",
          "telephone": "+31 123 456789"
        }
        </script>
      </head>
    </html>
    """

    rows = extract_from_html({
        "canonical_url": "https://acme.com",
        "success": True,
        "html": html,
        "markdown": "",
    })

    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("company_name", "Acme Manufacturing") in values
    assert ("company_legal_names", "Acme Manufacturing GmbH") in values
    assert ("primary_email", "info@acme.com") in values


def test_extract_jsonld_country_name_creates_country_name_and_code():
    html = """
    <html>
      <head>
        <script type="application/ld+json">
        {
          "@type": "Organization",
          "name": "Acme Manufacturing",
          "address": {
            "addressCountry": "United States",
            "addressLocality": "New York"
          }
        }
        </script>
      </head>
    </html>
    """

    rows = extract_from_html({
        "canonical_url": "https://acme.com",
        "success": True,
        "html": html,
        "markdown": "",
    })
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("main_country_code", "US") in values
    assert ("main_country", "United States") in values
    assert ("main_city", "New York") in values


def test_extract_jsonld_country_code_creates_country_name_and_code():
    html = """
    <html>
      <head>
        <script type="application/ld+json">
        {
          "@type": "Organization",
          "name": "Acme Manufacturing",
          "address": {
            "addressCountry": "US"
          }
        }
        </script>
      </head>
    </html>
    """

    rows = extract_from_html({
        "canonical_url": "https://acme.com",
        "success": True,
        "html": html,
        "markdown": "",
    })
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("main_country_code", "US") in values
    assert ("main_country", "United States") in values


def test_extract_jsonld_year_founded_and_employee_count():
    html = """
    <html>
      <head>
        <script type="application/ld+json">
        {
          "@type": "Organization",
          "name": "Acme Manufacturing",
          "foundingDate": "1998-04-01",
          "numberOfEmployees": {
            "@type": "QuantitativeValue",
            "value": "1,250"
          }
        }
        </script>
      </head>
    </html>
    """

    rows = extract_from_html({
        "canonical_url": "https://acme.com",
        "success": True,
        "html": html,
        "markdown": "",
    })
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("year_founded", "1998") in values
    assert ("employee_count", "1250") in values


def test_extract_geo_from_meta_and_body_text():
    html = """
    <html>
      <head>
        <meta name="geo.placename" content="Copenhagen">
        <meta name="description" content="Headquartered in Copenhagen, Denmark">
      </head>
      <body>
        <main>Acme serves manufacturers across Denmark.</main>
      </body>
    </html>
    """

    rows = extract_from_html({
        "canonical_url": "https://acme.dk",
        "success": True,
        "html": html,
        "markdown": "",
    })
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("main_country_code", "DK") in values
    assert ("main_country", "Denmark") in values
    assert ("main_city", "Copenhagen") in values


def test_extract_email_from_markdown():
    rows = extract_from_html({
        "canonical_url": "https://acme.com",
        "success": True,
        "html": "<html></html>",
        "markdown": "Contact us at sales@acme.com",
    })

    assert rows[0]["field_name"] == "primary_email"
    assert rows[0]["extracted_value"] == "sales@acme.com"


def test_extract_year_founded_and_employee_count_from_text():
    rows = extract_from_html({
        "canonical_url": "https://acme.com",
        "success": True,
        "html": """
        <html>
          <body>
            <main>Acme was founded in 2005 and has 120 employees worldwide.</main>
          </body>
        </html>
        """,
        "markdown": "",
    })
    values = {(row["field_name"], row["extracted_value"]) for row in rows}

    assert ("year_founded", "2005") in values
    assert ("employee_count", "120") in values
