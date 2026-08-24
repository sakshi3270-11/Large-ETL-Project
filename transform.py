import pandas as pd
import logging


# Get logger created by the main pipeline
logger = logging.getLogger("ETL_PIPELINE")


def transform_data(df):

    logger.info("Starting Transform step...")

    # 1. Convert transaction_date to datetime
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"]
    )

    logger.info(
        "transaction_date converted to datetime"
    )

    # 2. Clean text columns
    text_columns = [
        "customer_name",
        "product",
        "category",
        "payment_method",
        "status",
        "region"
    ]

    for column in text_columns:

        df[column] = (
            df[column]
            .astype(str)
            .str.strip()
        )

    logger.info("Text columns cleaned")

    # 3. Standardize status to uppercase
    df["status"] = df["status"].str.upper()

    logger.info("Status standardized to uppercase")

    # 4. Recalculate total amount
    df["total_amount"] = (
        df["quantity"] *
        df["unit_price"]
    )

    logger.info("total_amount recalculated")

    # 5. Add transaction year
    df["transaction_year"] = (
        df["transaction_date"].dt.year
    )

    # 6. Add transaction month
    df["transaction_month"] = (
        df["transaction_date"].dt.month
    )

    # 7. Add successful transaction flag
    df["is_successful"] = (
        df["status"] == "SUCCESS"
    )

    # 8. Add ETL processing timestamp
    df["processed_date"] = pd.Timestamp.now()

    logger.info(
        "New columns added: "
        "transaction_year, "
        "transaction_month, "
        "is_successful, "
        "processed_date"
    )

    logger.info(
        f"Records after transformation: {len(df)}"
    )

    return df