import multiprocessing

import email_validator
import phonenumbers
import typer
from loguru import logger


import pandas as pd
from sqlalchemy import create_engine

import pandas as pd

from src.config import CLICKHOUSE_URI


app = typer.Typer()


def preprocess_full_name(full_name: str) -> tuple[str, str, str]:
    """Preprocesses a full name string by splitting it into first, middle, and last names."""
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
    except email_validator.EmailNotValidError:
        return None


def parse_date(date_str: str):
    """approximate date format: YYYY-MM-DD"""
    date_str = date_str.strip()
    try:
        year, month, day = date_str.split("-")
    except ValueError:
        return None

    if len(year) == 2:
        if int(year) > 21:
            year = "19" + year
        else:
            year = "20" + year
    elif len(year) == 3:
        if int(year[:0]) == 9:
            year = "1" + year
        else:
            year = "2" + year

    if len(month) == 1 and month != "0":
        month = "0" + month
    elif not 1 < int(month) < 12:
        month = "01"

    if len(day) == 1 and day != "0":
        day = "0" + day
    elif not 1 < int(day) < 31:
        day = "01"

    return f"{year}-{month}-{day}"


def preprocess_type_1(df: pd.DataFrame):
    """Preprocess for type 1 dataset."""
    logger.info("Preprocessing type 1 dataset.")

    df[["first_name", "middle_name", "last_name"]] = (
        df["full_name"].apply(preprocess_full_name).apply(pd.Series)
    )

    df.drop(columns=["full_name"], inplace=True)
    df["phone"] = df["phone"].apply(preprocess_phone_number)
    df["email"] = df["email"].apply(preprocess_email)
    # rename column uid to unique_id
    df.rename(columns={"uid": "unique_id"}, inplace=True)
    logger.success("Preprocessing type 1 dataset complete.")
    return df


def preprocess_type_2(df: pd.DataFrame):
    """Preprocess for type 2 dataset."""
    logger.info("Preprocessing type 2 dataset.")
    df.rename(columns={"uid": "unique_id"}, inplace=True)

    df[["first_name", "middle_name", "last_name"]] = (
        df["full_name"].apply(preprocess_full_name).apply(pd.Series)
    )

    df.drop(columns=["full_name"], inplace=True)

    df["birthdate"] = df["birthdate"].astype(str).str.replace(r"[^\d\-\/\.]", "", regex=True)

    df["birthdate"] = df["birthdate"].apply(parse_date)

    df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)

    df["phone"] = df["phone"].apply(preprocess_phone_number)

    df["address"] = df["address"].astype(str).str.replace(r"\n", " ", regex=True)
    df["address"] = df["address"].str.replace(r"\s+", " ", regex=True).str.strip()

    logger.success("Preprocessing type 2 dataset complete.")
    return df


def preprocess_type_3(df: pd.DataFrame):
    """Preprocess for type 3 dataset."""
    logger.info("Preprocessing type 2 dataset.")

    df.rename(columns={"uid": "unique_id"}, inplace=True)

    # df['name'] = df[['first_name', 'middle_name', 'last_name']].fillna('').agg(' '.join, axis=1)
    df["name"] = df["name"].str.replace(r"\s+", " ", regex=True).str.strip()
    df["name"] = df["name"].str.replace(r"[^A-Za-zА-Яа-яЁё\s\-]", "", regex=True)

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

    name_splits = df["name"].apply(split_name)
    df["first_name"], df["middle_name"], df["last_name"] = zip(*name_splits)

    for col in ["first_name", "middle_name", "last_name"]:
        df[col] = df[col].str.title().str.strip()

    df.drop(columns=["name"], inplace=True)

    df["birthdate"] = df["birthdate"].astype(str).str.replace(r"[^\d\-\/\.]", "", regex=True)

    df["birthdate"] = df["birthdate"].apply(parse_date)

    logger.success("Preprocessing type 3 dataset complete.")
    return df


@app.command()
def main(
    clickhouse_uri: str = typer.Option("", help="ClickHouse URI."),
):
    """Preprocessing for the dataset."""

    if not clickhouse_uri:
        clickhouse_uri = CLICKHOUSE_URI

    # ---- REPLACE THIS WITH YOUR OWN CODE ----

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


# -----------------------------------------


if __name__ == "__main__":
    app()
