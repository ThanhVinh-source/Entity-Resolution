import os
import shutil

import pandas as pd

# PANDAS IO FUNCTIONS
# Reads an input CSV file into a pandas DataFrame
def read_input_csv(path):
    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=False,
    )
    return df

# Reads an input Parquet file into a pandas DataFrame
def read_parquet(path):
    return pd.read_parquet(path)

# Prepares the output path for writing by deleting existing files or directories if the mode is set to "overwrite", ensuring that the new output can be written without conflicts.
def prepare_output_path(path, mode):
    if mode != "overwrite":
        return

    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)

    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

# Writes a pandas DataFrame to a Parquet file at the specified path, using the prepare_output_path function to handle existing files based on the specified mode.
def write_parquet(df, path, mode="overwrite"):
    prepare_output_path(path, mode)
    df.to_parquet(path, index=False)

# Writes a pandas DataFrame to a CSV file at the specified path, using the prepare_output_path function to handle existing files based on the specified mode.
def write_csv(df, path, mode="overwrite"):
    prepare_output_path(path, mode)
    df.to_csv(path, index=False)
