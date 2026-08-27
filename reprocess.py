import logging
import os
import pandas as pd

from validate import validate_data
from transform import transform_data
from load import reprocess_load_data


logger = logging.getLogger("ETL_REPROCESS")


# ==================================================
# LOGGING SETUP
# ==================================================

def setup_logging():

    os.makedirs(
        "logs",
        exist_ok=True
    )

    logger.setLevel(
        logging.INFO
    )

    logger.propagate = False

    # Remove existing handlers
    if logger.handlers:
        logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(
        "logs/reprocessing.log",
        encoding="utf-8"
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(
        formatter
    )

    logger.addHandler(
        file_handler
    )


# ==================================================
# REPROCESS RECORDS
# ==================================================

def reprocess_records(file_path):

    logger.info("=" * 50)

    logger.info(
        "REJECTED RECORD REPROCESSING STARTED"
    )

    logger.info("=" * 50)

    try:

        # --------------------------------------------------
        # READ CORRECTED RECORDS
        # --------------------------------------------------

        df = pd.read_csv(
            file_path
        )

        logger.info(
            f"Corrected records read: {len(df)}"
        )

        # --------------------------------------------------
        # VALIDATION
        # --------------------------------------------------

        valid_df, rejected_df = validate_data(
            df,
            save_rejections=False
        )

        if not rejected_df.empty:

            logger.error(
                f"Validation failed: "
                f"{len(rejected_df)} record(s) rejected"
            )

            logger.error(
                "Reprocessing stopped."
            )

            return

        logger.info(
            f"Validation passed: {len(valid_df)} record(s)"
        )

        # --------------------------------------------------
        # TRANSFORM
        # --------------------------------------------------

        transformed_df = transform_data(
            valid_df
        )

        logger.info(
            f"Transformation completed: "
            f"{len(transformed_df)} record(s)"
        )

        # --------------------------------------------------
        # LOAD / UPDATE
        # --------------------------------------------------

        updated, inserted = reprocess_load_data(
            transformed_df
        )

        # --------------------------------------------------
        # FINAL SUMMARY
        # --------------------------------------------------

        logger.info(
            f"REPROCESSING SUMMARY | "
            f"Updated: {updated} | "
            f"Inserted: {inserted}"
        )

        logger.info("=" * 50)

        logger.info(
            "REJECTED RECORD REPROCESSING "
            "COMPLETED SUCCESSFULLY"
        )

        logger.info("=" * 50)

    except Exception as e:

        logger.exception(
            f"REPROCESSING FAILED: {e}"
        )

        raise


# ==================================================
# PROGRAM START
# ==================================================

if __name__ == "__main__":

    setup_logging()

    file_path = (
        "Reprocess/corrected_records.csv"
    )

    reprocess_records(
        file_path
    )