import logging
import os

# Set up logging configuration(Set the logging level to INFO)
logging.basicConfig(level=logging.INFO)

# Get the logger
logger = logging.getLogger()


# Define the database configuration
# Define the database configuration
db_config = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432,
    }

FOLDER_PATH_ASSIST = os.getenv("DESTINATION_PATH_ASSIST", r"C:\DESTINATION_PATH\ASSIST")
FOLDER_PATH_DAaas = os.getenv("DESTINATION_PATH_DAaas", r"C:\DESTINATION_PATH\DAaas")
FOLDER_PATH_DAaas_How_to = os.getenv("DESTINATION_PATH_DAaas_How_to", r"C:\DESTINATION_PATH\DAaas How to")

categories = {
    "ASSIST": FOLDER_PATH_ASSIST,
    "DAaaS": FOLDER_PATH_DAaas,
    "DAaaS How To": FOLDER_PATH_DAaas_How_to
}
