import logging
import pyodbc

from config import get_connection_string


logger = logging.getLogger("ETL_PIPELINE")


def load_data(df):

    logger.info("Starting Load step...")

    conn = None
    cursor = None

    inserted = 0
    skipped = 0

    try:

        # ==================================================
        # DATABASE CONNECTION
        # ==================================================

        conn = pyodbc.connect(
            get_connection_string()
        )

        cursor = conn.cursor()

        logger.info(
            "Connected successfully to SalesETLDB"
        )

        # ==================================================
        # GET EXISTING TRANSACTION IDs
        # ==================================================

        cursor.execute(
            """
            SELECT transaction_id
            FROM sales_transactions
            """
        )

        existing_ids = {
            row[0]
            for row in cursor.fetchall()
        }

        logger.info(
            f"Existing records in database: "
            f"{len(existing_ids)}"
        )

        # ==================================================
        # FIND NEW RECORDS
        # ==================================================

        new_df = df[
            ~df["transaction_id"].isin(existing_ids)
        ].copy()

        skipped = len(df) - len(new_df)

        logger.info(
            f"New records to load: {len(new_df)}"
        )

        logger.info(
            f"Existing records to skip: {skipped}"
        )

        # ==================================================
        # NOTHING NEW TO LOAD
        # ==================================================

        if new_df.empty:

            logger.info(
                "No new records found. Nothing to load."
            )

            return inserted, skipped

        # ==================================================
        # INSERT QUERY
        # ==================================================

        insert_query = """
        INSERT INTO sales_transactions (
            transaction_id,
            customer_id,
            customer_name,
            product,
            category,
            quantity,
            unit_price,
            total_amount,
            payment_method,
            status,
            transaction_date,
            region,
            transaction_year,
            transaction_month,
            is_successful,
            processed_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # ==================================================
        # BATCH PROCESSING
        # ==================================================

        batch_size = 10000

        total_new_records = len(new_df)

        for start in range(
            0,
            total_new_records,
            batch_size
        ):

            end = min(
                start + batch_size,
                total_new_records
            )

            batch = new_df.iloc[start:end]

            records = []

            # ==================================================
            # PREPARE RECORDS
            # ==================================================

            for _, row in batch.iterrows():

                records.append((
                    row["transaction_id"],
                    row["customer_id"],
                    row["customer_name"],
                    row["product"],
                    row["category"],
                    int(row["quantity"]),
                    float(row["unit_price"]),
                    float(row["total_amount"]),
                    row["payment_method"],
                    row["status"],
                    row["transaction_date"],
                    row["region"],
                    int(row["transaction_year"]),
                    int(row["transaction_month"]),
                    bool(row["is_successful"]),
                    row["processed_date"]
                ))

            # ==================================================
            # INSERT BATCH
            # ==================================================

            cursor.executemany(
                insert_query,
                records
            )

            conn.commit()

            inserted += len(records)

            logger.info(
                f"Loaded new records "
                f"{start + 1} to {end}"
            )

        # ==================================================
        # LOAD SUMMARY
        # ==================================================

        logger.info("=" * 50)
        logger.info("LOAD SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Inserted: {inserted}")
        logger.info(f"Skipped : {skipped}")

        return inserted, skipped

    except Exception as e:

        logger.exception(
            f"Load failed: {e}"
        )

        if conn is not None:

            conn.rollback()

            logger.info(
                "Database changes rolled back"
            )

        raise

    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:

            conn.close()

            logger.info(
                "Database connection closed"
            )