import logging
import os

from extract import extract_data
from validate import validate_data
from transform import transform_data
from load import load_data
from audit import create_audit_run, update_audit_run


# ==================================================
# LOGGING CONFIGURATION
# ==================================================

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True
)

logger = logging.getLogger("ETL_PIPELINE")


def main():

    logger.info("=" * 50)
    logger.info("SALES ETL PIPELINE STARTED")
    logger.info("=" * 50)

    # ==================================================
    # AUDIT VARIABLES
    # ==================================================

    run_id = None

    records_extracted = 0
    records_validated = 0
    records_rejected = 0
    records_inserted = 0
    records_skipped = 0

    try:

        # ==================================================
        # CREATE AUDIT RUN
        # ==================================================

        run_id = create_audit_run()

        file_path = "data/sales_transactions_100000.csv"

        # ==================================================
        # EXTRACT
        # ==================================================

        df = extract_data(file_path)

        records_extracted = len(df)

        # ==================================================
        # VALIDATE
        # ==================================================

        valid_df, invalid_df = validate_data(df)

        records_validated = len(valid_df)
        records_rejected = len(invalid_df)

        logger.info(
            f"VALIDATION SUMMARY | "
            f"Valid: {records_validated} | "
            f"Rejected: {records_rejected}"
        )

        # ==================================================
        # TRANSFORM
        # ==================================================

        transformed_df = transform_data(valid_df)

        # ==================================================
        # LOAD
        # ==================================================

        records_inserted, records_skipped = load_data(
            transformed_df
        )

        # ==================================================
        # UPDATE AUDIT - SUCCESS
        # ==================================================

        update_audit_run(
            run_id=run_id,
            records_extracted=records_extracted,
            records_validated=records_validated,
            records_rejected=records_rejected,
            records_inserted=records_inserted,
            records_skipped=records_skipped,
            status="SUCCESS",
            error_message=None
        )

        # ==================================================
        # PIPELINE SUCCESS
        # ==================================================

        logger.info("=" * 50)
        logger.info(
            "ETL PIPELINE COMPLETED SUCCESSFULLY"
        )
        logger.info("=" * 50)

    except Exception as e:

        # ==================================================
        # PIPELINE FAILURE
        # ==================================================

        logger.exception(
            f"ETL PIPELINE FAILED: {e}"
        )

        # ==================================================
        # UPDATE AUDIT - FAILED
        # ==================================================

        if run_id is not None:

            try:

                update_audit_run(
                    run_id=run_id,
                    records_extracted=records_extracted,
                    records_validated=records_validated,
                    records_rejected=records_rejected,
                    records_inserted=records_inserted,
                    records_skipped=records_skipped,
                    status="FAILED",
                    error_message=str(e)
                )

            except Exception as audit_error:

                logger.exception(
                    f"Failed to update audit record: {audit_error}"
                )

        logger.info("=" * 50)
        logger.info("ETL PIPELINE STOPPED")
        logger.info("=" * 50)

        raise


if __name__ == "__main__":
    main()