import re
from urllib.parse import urlparse


GENERIC_TLDS = {"com", "net", "org", "io", "co", "biz", "info", "app"}

TLD_TO_ISO = {
    "dk": "DK",
    "no": "NO",
    "se": "SE",
    "fi": "FI",
    "is": "IS",
    "pk": "PK",
    "in": "IN",
    "sg": "SG",
    "my": "MY",
    "th": "TH",
    "hk": "HK",
    "cn": "CN",
    "jp": "JP",
    "kr": "KR",
    "au": "AU",
    "nz": "NZ",
    "ie": "IE",
    "gb": "GB",
    "co.uk": "GB",
    "de": "DE",
    "fr": "FR",
    "nl": "NL",
    "be": "BE",
    "ch": "CH",
    "at": "AT",
    "es": "ES",
    "it": "IT",
    "pt": "PT",
    "pl": "PL",
    "cz": "CZ",
    "sk": "SK",
    "hu": "HU",
    "ro": "RO",
    "hr": "HR",
    "lv": "LV",
    "lt": "LT",
    "ee": "EE",
    "us": "US",
    "ca": "CA",
    "mx": "MX",
    "br": "BR",
    "ar": "AR",
    "cl": "CL",
    "co": "CO",
    "za": "ZA",
    "ng": "NG",
    "ke": "KE",
    "eg": "EG",
    "lu": "LU",
    "tr": "TR",
    "bd": "BD",
    "lk": "LK",
    "ph": "PH",
    "id": "ID",
    "vn": "VN",
    "ae": "AE",
    "com.pk": "PK",
    "com.sg": "SG",
    "com.my": "MY",
    "co.th": "TH",
    "com.au": "AU",
    "com.bd": "BD",
    "com.hk": "HK",
}

LANG_TO_ISO = {
    "da": "DK",
    "no": "NO",
    "nb": "NO",
    "nn": "NO",
    "sv": "SE",
    "fi": "FI",
    "is": "IS",
    "de": "DE",
    "fr": "FR",
    "nl": "NL",
    "it": "IT",
    "es": "ES",
    "pt": "PT",
    "pl": "PL",
    "cs": "CZ",
    "sk": "SK",
    "hu": "HU",
    "ro": "RO",
    "hr": "HR",
    "lv": "LV",
    "lt": "LT",
    "et": "EE",
    "zh": "CN",
    "ja": "JP",
    "ko": "KR",
    "th": "TH",
    "ms": "MY",
    "id": "ID",
    "vi": "VN",
    "tr": "TR",
    "ar": "EG",
    "ru": "RU",
}

ISO_TO_COUNTRY_NAME = {
    "DK": "Denmark",
    "NO": "Norway",
    "SE": "Sweden",
    "FI": "Finland",
    "IS": "Iceland",
    "PK": "Pakistan",
    "IN": "India",
    "SG": "Singapore",
    "MY": "Malaysia",
    "TH": "Thailand",
    "HK": "Hong Kong",
    "CN": "China",
    "JP": "Japan",
    "KR": "South Korea",
    "AU": "Australia",
    "NZ": "New Zealand",
    "IE": "Ireland",
    "GB": "United Kingdom",
    "DE": "Germany",
    "FR": "France",
    "NL": "Netherlands",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "ES": "Spain",
    "IT": "Italy",
    "PT": "Portugal",
    "PL": "Poland",
    "CZ": "Czechia",
    "SK": "Slovakia",
    "HU": "Hungary",
    "RO": "Romania",
    "HR": "Croatia",
    "LV": "Latvia",
    "LT": "Lithuania",
    "EE": "Estonia",
    "US": "United States",
    "CA": "Canada",
    "MX": "Mexico",
    "BR": "Brazil",
    "AR": "Argentina",
    "CL": "Chile",
    "CO": "Colombia",
    "ZA": "South Africa",
    "NG": "Nigeria",
    "KE": "Kenya",
    "EG": "Egypt",
    "LU": "Luxembourg",
    "TR": "Turkey",
    "BD": "Bangladesh",
    "LK": "Sri Lanka",
    "PH": "Philippines",
    "ID": "Indonesia",
    "VN": "Vietnam",
    "RU": "Russia",
    "AE": "United Arab Emirates",
}

TEXT_SIGNALS = {
    "DK": ["denmark", "danish", "aalborg", "copenhagen", "aarhus", "odense", "danmark"],
    "NO": ["norway", "norwegian", "oslo", "bergen", "trondheim", "norge", "norsk"],
    "SE": ["sweden", "swedish", "stockholm", "gothenburg", "malmo", "sverige"],
    "FI": ["finland", "finnish", "helsinki", "tampere", "turku", "suomi"],
    "PK": ["pakistan", "pakistani", "karachi", "lahore", "islamabad", "rawalpindi"],
    "IN": ["india", "indian", "mumbai", "delhi", "bangalore", "bengaluru", "hyderabad", "chennai", "pune"],
    "SG": ["singapore", "singaporean"],
    "MY": ["malaysia", "malaysian", "kuala lumpur", "penang", "johor"],
    "TH": ["thailand", "thai", "bangkok", "pattaya"],
    "HK": ["hong kong"],
    "CN": ["china", "chinese", "beijing", "shanghai", "shenzhen"],
    "GB": ["united kingdom", "uk based", "great britain", "england", "scotland", "london", "manchester"],
    "IE": ["ireland", "irish", "dublin", "cork", "limerick"],
    "DE": ["germany", "german", "berlin", "munich", "hamburg", "frankfurt"],
    "FR": ["france", "french", "paris", "lyon", "marseille"],
    "NL": ["netherlands", "dutch", "amsterdam", "rotterdam", "eindhoven"],
    "US": ["united states", "usa", "u s a", "u s", "new york", "san francisco", "los angeles", "chicago"],
    "CA": ["canada", "canadian", "toronto", "montreal", "vancouver"],
    "AU": ["australia", "australian", "sydney", "melbourne", "brisbane"],
    "BD": ["bangladesh", "bangladeshi", "dhaka", "chittagong"],
    "LU": ["luxembourg"],
    "CH": ["switzerland", "swiss", "zurich", "geneva", "bern"],
    "SK": ["slovakia", "slovak", "bratislava"],
    "HU": ["hungary", "hungarian", "budapest"],
}

CITY_PATTERNS = [
    r"(?:based|located|headquartered|offices?)\s+in\s+([A-Z][a-zA-Z\s]{2,25}?)[\.,\s]",
    r"([A-Z][a-zA-Z]{2,15}),\s*(?:Denmark|Norway|Sweden|Pakistan|India|Singapore|Malaysia|Finland|Germany|Ireland|Netherlands|United States|Canada|Australia)",
    r"(?:^|\s)([A-Z][a-zA-Z]{3,15}),\s+[A-Z]{2}(?:\s|$)",
]


# note: Normalize strings before comparing country aliases, language tags, and text signals.
def normalize_lookup_value(value):
    if value is None:
        return None

    cleaned = str(value).strip().lower()
    if cleaned == "":
        return None

    cleaned = cleaned.replace("&", " and ")
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


COUNTRY_ALIASES = {
    normalize_lookup_value(country_name): country_code
    for country_code, country_name in ISO_TO_COUNTRY_NAME.items()
}
COUNTRY_ALIASES.update(
    {
        "usa": "US",
        "u s a": "US",
        "u s": "US",
        "united states of america": "US",
        "uk": "GB",
        "u k": "GB",
        "great britain": "GB",
        "england": "GB",
        "south korea": "KR",
        "republic of korea": "KR",
        "uae": "AE",
        "u a e": "AE",
        "czech republic": "CZ",
        "turkiye": "TR",
    }
)


# note: Convert a country name, ISO code, or locale-like value into an ISO-2 code.
def normalize_country_to_iso(value):
    if value is None:
        return None

    raw_value = str(value).strip()
    if raw_value == "":
        return None

    upper_value = raw_value.upper()
    if upper_value in ISO_TO_COUNTRY_NAME:
        return upper_value

    if re.fullmatch(r"[A-Z]{2}[-_][A-Z0-9]{2,3}", upper_value):
        country_code = upper_value[:2]
        if country_code in ISO_TO_COUNTRY_NAME:
            return country_code

    if re.fullmatch(r"[a-zA-Z]{2}[-_][a-zA-Z]{2}", raw_value):
        country_code = raw_value[-2:].upper()
        if country_code in ISO_TO_COUNTRY_NAME:
            return country_code

    normalized = normalize_lookup_value(raw_value)
    if normalized in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[normalized]

    return None


# note: Convert an ISO-2 country code into the canonical full country name used in the enriched dataset.
def country_name_from_iso(country_code):
    if country_code is None:
        return None

    return ISO_TO_COUNTRY_NAME.get(str(country_code).strip().upper())


# note: Normalize a raw TLD value from CSV into the lookup format used by the signal maps.
def normalize_tld(value):
    if value is None:
        return None

    tld = str(value).lower().strip().lstrip(".")
    if tld == "":
        return None

    return tld


# note: Extract the most specific known TLD from a URL or domain, including compound TLDs.
def extract_tld_from_url(url):
    if url is None:
        return None

    value = str(url).strip()
    if value == "":
        return None

    if not value.startswith(("http://", "https://")):
        value = "https://" + value

    parsed = urlparse(value)
    domain = parsed.netloc.lower().split("@")[-1].split(":")[0]
    if domain.startswith("www."):
        domain = domain[4:]

    for tld in sorted(TLD_TO_ISO, key=len, reverse=True):
        if domain == tld or domain.endswith("." + tld):
            return tld

    return None


# note: Return only strong country signals from country-specific TLDs, never from generic domains.
def infer_strong_country_from_tld(website_tld=None, website_url=None):
    tld = normalize_tld(website_tld)
    source = "website_tld"

    if tld is None:
        tld = extract_tld_from_url(website_url)
        source = "website_url"

    country_code = TLD_TO_ISO.get(tld)
    if country_code and tld not in GENERIC_TLDS:
        return country_code, tld, source

    return None, tld, source


# note: Infer a weak or strong country signal from CSV metadata without crawling the web.
def infer_country_from_csv_fields(website_tld, website_language_code):
    language = str(website_language_code or "").lower().strip().replace("_", "-")
    tld_country, _, _ = infer_strong_country_from_tld(website_tld)
    if tld_country:
        return tld_country, "tld"

    language_parts = [part for part in language.split("-") if part]
    if len(language_parts) >= 2:
        locale_country = normalize_country_to_iso(language_parts[-1])
        if locale_country:
            return locale_country, "language"

    if language_parts:
        language_country = LANG_TO_ISO.get(language_parts[0])
        if language_country:
            return language_country, "language"

    return None, None


# note: Pull a likely city from short body text snippets and address-like phrases.
def extract_city(text):
    if not text:
        return None

    for pattern in CITY_PATTERNS:
        match = re.search(pattern, str(text))
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()

    return None


# note: Extract country and city evidence from title, metadata, markdown, and cleaned page text.
def extract_country_city_from_text(text, title=None, url=None):
    full_text = " ".join(
        str(value)
        for value in [title, text, url]
        if value is not None and str(value).strip() != ""
    )
    normalized_text = normalize_lookup_value(full_text)
    city = extract_city(full_text)

    if normalized_text is None:
        return {
            "country_code": None,
            "country_name": None,
            "city": city,
            "method": None,
        }

    padded_text = f" {normalized_text} "
    for country_code, signals in TEXT_SIGNALS.items():
        for signal in signals:
            normalized_signal = normalize_lookup_value(signal)
            if normalized_signal and f" {normalized_signal} " in padded_text:
                return {
                    "country_code": country_code,
                    "country_name": country_name_from_iso(country_code),
                    "city": city,
                    "method": "text_keyword",
                }

    return {
        "country_code": None,
        "country_name": None,
        "city": city,
        "method": None,
    }
