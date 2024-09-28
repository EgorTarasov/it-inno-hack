import multiprocessing

import email_validator
import pandas as pd
import phonenumbers
import typer
from loguru import logger
from sqlalchemy import create_engine
from tqdm import tqdm

from src.config import CLICKHOUSE_URI, PROCESSED_DATA_DIR, RAW_DOCKER_DIR
from datetime import datetime

app = typer.Typer()


def preprocess_full_name(full_name: str) -> tuple[str, str, str]:
    """Processes a full name string by splitting it into first, middle, and last names."""
    parts = full_name.lower().split()
    if len(parts) >= 3:
        return parts[0], " ".join(parts[1:-1]), parts[-1]
    elif len(parts) == 2:
        return parts[0], "", parts[1]
    elif len(parts) == 1:
        return parts[0], "", ""
    else:
        return "", "", ""


def preprocess_phone_number(phone_number: str) -> str | None:
    """
    Preprocesses a phone number string by replacing common misinterpretations of digits
    and formats it to the international phone number format.
    Args:
        phone_number (str): The phone number string to preprocess.
    Returns:
        str | None: The formatted international phone number string, or None if the number is invalid.
    """

    phone_number = (
        phone_number.replace("i", "1")
        .replace("l", "1")
        .replace("o", "0")
        .replace("s", "5")
        .replace("I", "1")
        .replace("O", "0")
        .replace("S", "5")
    )
    try:
        return phonenumbers.format_number(
            phonenumbers.parse(phone_number, "RU"), phonenumbers.PhoneNumberFormat.INTERNATIONAL
        )
    except phonenumbers.phonenumberutil.NumberParseException:
        return None


def preprocess_email(email: str) -> str | None:
    """Preprocess email. if email is not valid, return None. to not disturb the pipeline."""
    try:
        return email_validator.validate_email(email, check_deliverability=False).email
    except Exception as e:
        return None


def parse_date(date_str: str) -> str | None:
    """Approximate date format: YYYY-MM-DD"""
    date_str = date_str.strip()
    try:
        # Attempt to parse the date string directly
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    try:
        # Handle cases where year, month, or day might be incomplete
        parts = date_str.split("-")
        if len(parts) != 3:
            return None

        year, month, day = parts

        # Normalize year
        if len(year) == 2:
            year = "19" + year if int(year) > 21 else "20" + year
        elif len(year) == 3:
            year = "1" + year if year.startswith("9") else "2" + year
        elif len(year) != 4:
            return None

        # Normalize month
        if len(month) == 1:
            month = "0" + month
        elif not (1 <= int(month) <= 12):
            return None

        # Normalize day
        if len(day) == 1:
            day = "0" + day
        elif not (1 <= int(day) <= 31):
            return None

        return f"{year}-{month}-{day}"
    except Exception:
        return None


def preprocess_type_1(df: pd.DataFrame):
    """Preprocess for type 1 dataset."""
    logger.info("Preprocessing type 1 dataset.")
    df.rename(columns={"uid": "unique_id"}, inplace=True)
    chunk_size = 1000  # Define the chunk size
    chunks = [df[i : i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

    processed_chunks = []
    for chunk in tqdm(chunks, desc="Processing chunks"):
        chunk.loc[:, ["first_name", "middle_name", "last_name"]] = (
            chunk["full_name"].apply(preprocess_full_name).apply(pd.Series)
        )

        chunk.loc[:, "phone"] = chunk["phone"].apply(preprocess_phone_number)
        chunk.loc[:, "email"] = chunk["email"].apply(preprocess_email)

        processed_chunks.append(chunk)
    df.drop(columns=["full_name"], inplace=True)
    df = pd.concat(processed_chunks, ignore_index=True)

    logger.success("Preprocessing type 1 dataset complete.")
    return df


def preprocess_type_2(df: pd.DataFrame):
    """Preprocess for type 2 dataset."""
    logger.info("Preprocessing type 2 dataset.")
    df.rename(columns={"uid": "unique_id"}, inplace=True)
    chunk_size = 1000  # Define the chunk size
    chunks = [df[i : i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

    processed_chunks = []
    for chunk in tqdm(chunks, desc="Processing chunks"):

        chunk.loc[:, ["first_name", "middle_name", "last_name"]] = (
            chunk["full_name"].apply(preprocess_full_name).apply(pd.Series)
        )

        chunk.loc[:, "birthdate"] = (
            chunk["birthdate"].astype(str).str.replace(r"[^\d\-\/\.]", "", regex=True)
        )
        chunk.loc[:, "birthdate"] = chunk["birthdate"].apply(parse_date)

        chunk.loc[:, "phone"] = chunk["phone"].astype(str).str.replace(r"\D", "", regex=True)
        chunk.loc[:, "phone"] = chunk["phone"].apply(preprocess_phone_number)

        chunk.loc[:, "address"] = chunk["address"].astype(str).str.replace(r"\n", " ", regex=True)
        chunk.loc[:, "address"] = chunk["address"].str.replace(r"\s+", " ", regex=True).str.strip()

        processed_chunks.append(chunk)

    df = pd.concat(processed_chunks, ignore_index=True)
    df.drop(columns=["full_name"], inplace=True)

    logger.success("Preprocessing type 2 dataset complete.")
    return df


def preprocess_type_3(df: pd.DataFrame):
    """Preprocess for type 3 dataset."""
    logger.info("Preprocessing type 3 dataset.")
    df.rename(columns={"uid": "unique_id"}, inplace=True)
    chunk_size = 1000  # Define the chunk size
    chunks = [df[i : i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

    processed_chunks = []
    for chunk in tqdm(chunks, desc="Processing chunks"):

        chunk.loc[:, "name"] = chunk["name"].str.replace(r"\s+", " ", regex=True).str.strip()
        chunk.loc[:, "name"] = chunk["name"].str.replace(r"[^A-Za-zА-Яа-яЁё\s\-]", "", regex=True)

        def split_name(full_name):
            parts = full_name.split()
            if len(parts) >= 3:
                return parts[0], " ".join(parts[1:-1]), parts[-1]
            elif len(parts) == 2:
                return parts[0], "", parts[1]
            elif len(parts) == 1:
                return parts[0], "", ""
            else:
                return "", "", ""

        name_splits = chunk["name"].apply(split_name)
        chunk.loc[:, "first_name"], chunk.loc[:, "middle_name"], chunk.loc[:, "last_name"] = zip(
            *name_splits
        )

        for col in ["first_name", "middle_name", "last_name"]:
            chunk.loc[:, col] = chunk[col].str.title().str.strip()

        chunk.loc[:, "birthdate"] = (
            chunk["birthdate"].astype(str).str.replace(r"[^\d\-\/\.]", "", regex=True)
        )
        chunk.loc[:, "birthdate"] = chunk["birthdate"].apply(parse_date)

        processed_chunks.append(chunk)
    df.drop(columns=["name"], inplace=True)
    df = pd.concat(processed_chunks, ignore_index=True)

    logger.success("Preprocessing type 3 dataset complete.")
    return df


def load_dfs(clickhouse_uri: str) -> list[pd.DataFrame]:
    engine = create_engine(clickhouse_uri)

    query = "SELECT * FROM {table_name} TabSeparatedWithNamesAndTypes"
    dfs = [
        pd.read_sql(query.format(table_name=table_name), engine)
        for table_name in ["table_dataset1", "table_dataset2", "table_dataset3"]
    ]

    for i, df in enumerate(dfs):
        with multiprocessing.Pool() as pool:
            if i == 0:
                dfs[i] = pool.apply(preprocess_type_2, (df,))
            elif i == 1:
                dfs[i] = pool.apply(preprocess_type_3, (df,))
            else:
                # Assuming there's a preprocess_type_1 function for the first dataset
                dfs[i] = pool.apply(preprocess_type_1, (df,))
    return dfs


def load_csv(parallel: bool = False) -> list[pd.DataFrame]:
    """Load CSV files from the RAW_DOCKER_DIR directory."""
    dfs = [
        pd.read_csv(RAW_DOCKER_DIR / f"{dataset_name}.csv")
        for dataset_name in ["main1", "main2", "main3"]
    ]
    logger.info("start preprocessing")
    if parallel:
        for i, df in enumerate(dfs):
            with multiprocessing.Pool() as pool:
                if i == 1:
                    dfs[i] = pool.apply(preprocess_type_2, (df,))
                elif i == 2:
                    dfs[i] = pool.apply(preprocess_type_3, (df,))
                else:
                    # Assuming there's a preprocess_type_1 function for the first dataset
                    dfs[i] = pool.apply(preprocess_type_1, (df,))
    else:
        for i, df in enumerate(dfs):
            if i == 1:
                dfs[i] = preprocess_type_2(df)
            elif i == 2:
                dfs[i] = preprocess_type_3(df)
            else:
                # Assuming there's a preprocess_type_1 function for the first dataset
                dfs[i] = preprocess_type_1(df)
    return dfs


@app.command()
def main(
    local: bool = typer.Option(False, help="Load data from local CSV files."),
    parallel: bool = typer.Option(False, help="Enable parallel processing."),
    clickhouse_uri: str = typer.Option("", help="ClickHouse URI."),
    dataset_name: str = typer.Option("dataset", help="Name of the dataset."),
):
    """Preprocessing for the dataset."""
    if local:
        dfs = load_csv(parallel)
    elif not clickhouse_uri:
        clickhouse_uri = CLICKHOUSE_URI
        # ---- REPLACE THIS WITH YOUR OWN CODE ----
        dfs = load_dfs(clickhouse_uri)

    # concatenate all dataframes into one
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(PROCESSED_DATA_DIR / f"{dataset_name}.csv", index=False)

    logger.info("Data loaded successfully.")


if __name__ == "__main__":
    app()
