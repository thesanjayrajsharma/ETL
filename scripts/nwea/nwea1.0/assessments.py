import logging
import dotenv

from src.etl import run_data_pull
from src.config import setup_logging

# load environment variables
dotenv.load_dotenv()

def main():
    """Main function to run the data pull"""
    try:
        run_data_pull()
        logging.info("Data pull successful")
    except Exception as e:
        logging.error(f"Error occurred in data pull: {e}")
        raise

if __name__ == '__main__':
    setup_logging()
    main()