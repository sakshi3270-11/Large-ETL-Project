import pandas as pd
import logging


# Get logger created by the main pipeline
logger = logging.getLogger("ETL_PIPELINE")


def transform_data(
    df,
    quiet=False
):

    # --------------------------------------------------
    # TRANSFORM START
    # --------------------------------------------------

    if not quiet:
        logger.info(
            "Starting Transform step..."
        )

    # --------------------------------------------------
    # 1. CONVERT TRANSACTION DATE
    # --------------------------------------------------

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"]
    )

    if not quiet:
        logger.info(
            "transaction_date converted to datetime"
        )

    # --------------------------------------------------
    # 2. CLEAN TEXT COLUMNS
    # --------------------------------------------------

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

    if not quiet:
        logger.info(
            "Text columns cleaned"
        )

    # --------------------------------------------------
    # 3. STANDARDIZE STATUS
    # --------------------------------------------------

    df["status"] = (
        df["status"]
        .str.upper()
    )

    if not quiet:
        logger.info(
            "Status standardized to uppercase"
        )

    # --------------------------------------------------
    # 4. RECALCULATE TOTAL AMOUNT
    # --------------------------------------------------

    df["total_amount"] = (
        df["quantity"]
        *
        df["unit_price"]
    )

    if not quiet:
        logger.info(
            "total_amount recalculated"
        )

    # --------------------------------------------------
    # 5. ADD TRANSACTION YEAR
    # --------------------------------------------------

    df["transaction_year"] = (
        df["transaction_date"]
        .dt.year
    )

    # --------------------------------------------------
    # 6. ADD TRANSACTION MONTH
    # --------------------------------------------------

    df["transaction_month"] = (
        df["transaction_date"]
        .dt.month
    )

    # --------------------------------------------------
    # 7. ADD SUCCESS FLAG
    # --------------------------------------------------

    df["is_successful"] = (
        df["status"] == "SUCCESS"
    )

    # --------------------------------------------------
    # 8. ADD PROCESSING TIMESTAMP
    # --------------------------------------------------

    df["processed_date"] = (
        pd.Timestamp.now()
    )

    if not quiet:
        logger.info(
            "New columns added: "
            "transaction_year, "
            "transaction_month, "
            "is_successful, "
            "processed_date"
        )

        logger.info(
            f"Records after transformation: "
            f"{len(df)}"
        )

    return df