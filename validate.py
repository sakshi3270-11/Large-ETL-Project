import logging
import os
import pandas as pd


logger = logging.getLogger("ETL_PIPELINE")


def validate_data(df, save_rejections=True):

    logger.info("Starting Validation step...")

    # --------------------------------------------------
    # REQUIRED COLUMNS
    # --------------------------------------------------

    required_columns = [
        "transaction_id",
        "customer_id",
        "customer_name",
        "product",
        "category",
        "quantity",
        "unit_price",
        "total_amount",
        "payment_method",
        "status",
        "transaction_date",
        "region"
    ]

    missing_columns = []

    for column in required_columns:

        if column not in df.columns:
            missing_columns.append(column)

    if missing_columns:

        logger.error(
            f"Missing required columns: {missing_columns}"
        )

        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logger.info("Required columns check: PASSED")

    # --------------------------------------------------
    # CREATE VALIDATION ERROR COLUMN
    # --------------------------------------------------

    validation_errors = pd.Series(
        "",
        index=df.index,
        dtype="object"
    )

    # --------------------------------------------------
    # MISSING VALUES
    # --------------------------------------------------

    missing_values = df.isnull().sum().sum()

    logger.info(
        f"Total missing values: {missing_values}"
    )

    missing_mask = df[
        required_columns
    ].isnull().any(axis=1)

    validation_errors.loc[
        missing_mask
    ] += "Missing required value; "

    # --------------------------------------------------
    # DUPLICATE TRANSACTION IDs
    # --------------------------------------------------

    duplicate_mask = df[
        "transaction_id"
    ].duplicated(keep=False)

    duplicate_ids = duplicate_mask.sum()

    logger.info(
        f"Duplicate transaction IDs: {duplicate_ids}"
    )

    validation_errors.loc[
        duplicate_mask
    ] += "Duplicate transaction ID; "

    # --------------------------------------------------
    # INVALID QUANTITY
    # --------------------------------------------------

    invalid_quantity_mask = (
        df["quantity"].notna()
        &
        (df["quantity"] <= 0)
    )

    invalid_quantity = (
        invalid_quantity_mask.sum()
    )

    logger.info(
        f"Invalid quantity records: {invalid_quantity}"
    )

    validation_errors.loc[
        invalid_quantity_mask
    ] += "Invalid quantity; "

    # --------------------------------------------------
    # INVALID UNIT PRICE
    # --------------------------------------------------

    invalid_price_mask = (
        df["unit_price"].notna()
        &
        (df["unit_price"] <= 0)
    )

    invalid_price = (
        invalid_price_mask.sum()
    )

    logger.info(
        f"Invalid unit price records: {invalid_price}"
    )

    validation_errors.loc[
        invalid_price_mask
    ] += "Invalid unit price; "

    # --------------------------------------------------
    # INVALID TOTAL AMOUNT
    # --------------------------------------------------

    invalid_amount_mask = (
        df["total_amount"].notna()
        &
        (df["total_amount"] <= 0)
    )

    invalid_amount = (
        invalid_amount_mask.sum()
    )

    logger.info(
        f"Invalid total amount records: "
        f"{invalid_amount}"
    )

    validation_errors.loc[
        invalid_amount_mask
    ] += "Invalid total amount; "

    # --------------------------------------------------
    # TOTAL AMOUNT CALCULATION CHECK
    # --------------------------------------------------

    calculated_total = (
        df["quantity"] *
        df["unit_price"]
    )

    invalid_calculation_mask = (
        df["quantity"].notna()
        &
        df["unit_price"].notna()
        &
        df["total_amount"].notna()
        &
        (
            abs(
                df["total_amount"]
                - calculated_total
            ) > 0.01
        )
    )

    invalid_calculation = (
        invalid_calculation_mask.sum()
    )

    logger.info(
        "Invalid total amount calculation "
        f"records: {invalid_calculation}"
    )

    validation_errors.loc[
        invalid_calculation_mask
    ] += (
        "Total amount does not match "
        "quantity × unit price; "
    )

    # --------------------------------------------------
    # INVALID STATUS
    # --------------------------------------------------

    valid_statuses = [
        "SUCCESS",
        "FAILED",
        "PENDING"
    ]

    invalid_status_mask = (
        df["status"].notna()
        &
        ~df["status"].isin(valid_statuses)
    )

    invalid_status = (
        invalid_status_mask.sum()
    )

    logger.info(
        f"Invalid status records: "
        f"{invalid_status}"
    )

    validation_errors.loc[
        invalid_status_mask
    ] += "Invalid status; "

    # --------------------------------------------------
    # SEPARATE VALID AND INVALID RECORDS
    # --------------------------------------------------

    invalid_mask = (
        validation_errors != ""
    )

    valid_df = df[
        ~invalid_mask
    ].copy()

    invalid_df = df[
        invalid_mask
    ].copy()

    # --------------------------------------------------
    # CREATE REJECTED FOLDER
    # --------------------------------------------------

    if save_rejections:

        os.makedirs(
            "rejected",
            exist_ok=True
        )

    # --------------------------------------------------
    # SAVE COMPLETE INVALID RECORDS
    # --------------------------------------------------

    if (
        save_rejections
        and not invalid_df.empty
    ):

        invalid_df.to_csv(
            "rejected/rejected_records.csv",
            index=False
        )

        logger.warning(
            f"Rejected records saved: "
            f"{len(invalid_df)}"
        )

    # --------------------------------------------------
    # SAVE ERROR INFORMATION
    # --------------------------------------------------

    if (
        save_rejections
        and not invalid_df.empty
    ):

        error_df = pd.DataFrame({
            "transaction_id":
                invalid_df[
                    "transaction_id"
                ],

            "error":
                validation_errors[
                    invalid_mask
                ].str.rstrip("; ")
        })

        error_df.to_csv(
            "rejected/rejection_errors.csv",
            index=False
        )

        logger.warning(
            "Validation error details saved."
        )

    # --------------------------------------------------
    # VALIDATION SUMMARY
    # --------------------------------------------------

    logger.info(
        f"Valid records: "
        f"{len(valid_df)}"
    )

    logger.info(
        f"Rejected records: "
        f"{len(invalid_df)}"
    )

    logger.info(
        "Validation completed."
    )

    return valid_df, invalid_df