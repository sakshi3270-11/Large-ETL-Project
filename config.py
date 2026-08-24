import os
from dotenv import load_dotenv


# Load variables from .env file
load_dotenv()


def get_connection_string():

    driver = os.getenv("DB_DRIVER")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    trusted_connection = os.getenv(
        "DB_TRUSTED_CONNECTION"
    )

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection={trusted_connection};"
    )

    return connection_string