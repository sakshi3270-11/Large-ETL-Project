import logging
import pyodbc

from config import get_connection_string


logger = logging.getLogger("ETL_PIPELINE")


def create_audit_run():

    conn = None
    cursor = None

    try:

        conn = pyodbc.connect(
            get_connection_string()
        )

        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO etl_audit (
                start_time,
                status
            )
            OUTPUT INSERTED.run_id
            VALUES (
                GETDATE(),
                'RUNNING'
            )
            """
        )

        run_id = cursor.fetchone()[0]

        conn.commit()

        logger.info(
            f"Audit run created. Run ID: {run_id}"
        )

        return run_id

    except Exception as e:

        logger.exception(
            f"Failed to create audit run: {e}"
        )

        raise

    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()


def update_audit_run(
    run_id,
    records_extracted,
    records_validated,
    records_rejected,
    records_inserted,
    records_skipped,
    status,
    error_message=None
):

    conn = None
    cursor = None

    try:

        conn = pyodbc.connect(
            get_connection_string()
        )

        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE etl_audit
            SET
                end_time = GETDATE(),
                records_extracted = ?,
                records_validated = ?,
                records_rejected = ?,
                records_inserted = ?,
                records_skipped = ?,
                status = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            (
                records_extracted,
                records_validated,
                records_rejected,
                records_inserted,
                records_skipped,
                status,
                error_message,
                run_id
            )
        )

        conn.commit()

        logger.info(
            f"Audit run {run_id} updated successfully"
        )

    except Exception as e:

        logger.exception(
            f"Failed to update audit run {run_id}: {e}"
        )

        if conn is not None:
            conn.rollback()

            logger.info(
                f"Audit update rolled back for Run ID: {run_id}"
            )

        raise

    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()