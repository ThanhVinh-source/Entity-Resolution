import pandas as pd

from company_data_enrichment.pandas_io import read_input_csv, read_parquet, write_csv, write_parquet


def test_read_input_csv_keeps_strings(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        'id,postcode,description\n1,00123,"hello\nworld"\n',
        encoding="utf-8",
    )

    df = read_input_csv(csv_path)

    assert df.loc[0, "postcode"] == "00123"
    assert df.loc[0, "description"] == "hello\nworld"


def test_write_and_read_parquet(tmp_path):
    parquet_path = tmp_path / "sample.parquet"
    df = pd.DataFrame([{"id": "1", "name": "Acme"}])

    write_parquet(df, parquet_path)
    result = read_parquet(parquet_path)

    assert result.to_dict("records") == [{"id": "1", "name": "Acme"}]


def test_write_csv(tmp_path):
    csv_path = tmp_path / "sample.csv"
    df = pd.DataFrame([{"id": "1", "name": "Acme"}])

    write_csv(df, csv_path)
    result = pd.read_csv(csv_path, dtype=str)

    assert result.to_dict("records") == [{"id": "1", "name": "Acme"}]
