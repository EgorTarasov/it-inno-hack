from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm
import phonenumbers
import email_validator

from record_linkage.config import PROCESSED_DATA_DIR


app = typer.Typer()


def preprocess_phone_number(phone_number: str) -> str | None:
    try:
        phone_number = phonenumbers.parse(phone_number, None).
        return phone_number.national_number
    except phonenumbers.phonenumberutil.NumberParseException:
        return None


@app.command()
def main(
    clickhouse_uri: str = typer.Option("clickhouse://localhost:9000", help="ClickHouse URI."),
):
    """Preprocessing for the dataset."""

    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Processing dataset...")
    for i in tqdm(range(10), total=10):
        if i == 5:
            logger.info("Something happened for iteration 5.")
    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
