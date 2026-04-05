import argparse
from pyflow.extractors import CSVExtractor
from pyflow.transformers import transform_data
from pyflow.loaders import SQLiteLoader
from pyflow.logger import setup_logger
from pyflow.config_parser import load_config
import pandas as pd


def run_pipeline(config_path):

    logger = setup_logger()

    try:
        logger.info("Starting ETL Pipeline")

        # -------------------------------
        # LOAD CONFIG
        # -------------------------------
        config = load_config(config_path)

        file_path = config["file_path"]
        db_path = config["db_path"]
        table_name = config["table_name"]

        logger.info(f"Using config file: {config_path}")

        # -------------------------------
        # EXTRACT
        # -------------------------------
        extractor = CSVExtractor()
        df = extractor.extract(file_path)

        logger.info(f"Data extracted. Rows: {len(df)}")

        # -------------------------------
        # TRANSFORM
        # -------------------------------
        df = transform_data(df)

        logger.info(f"Data transformed. Rows: {len(df)}")

        # -------------------------------
        # LOAD
        # -------------------------------
        loader = SQLiteLoader(db_path)
        loader.load(df, table_name)

        logger.info("Data loaded successfully")

        print("\nPipeline executed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        print("Error occurred. Check logs.")


# --------------------------------------------------
# CLI ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run ETL Pipeline")

    parser.add_argument(
        "--config",
        type=str,
        default="config/config.json",
        help="Path to config file"
    )

    args = parser.parse_args()

    run_pipeline(args.config)