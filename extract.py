import pandas as pd
import logging


# Get logger created by the main pipeline
logger = logging.getLogger("ETL_PIPELINE")


def extract_data(file_path):

    logger.info("Starting Extract step...")

    df = pd.read_csv(file_path)

    logger.info(
        f"Records extracted: {len(df)}"
    )

    logger.info(
        f"Columns found: {len(df.columns)}"
    )

    return df