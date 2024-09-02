import logging
import os
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from src.task1 import process_it_data
from src.task2 import process_marketing_address_info
from src.task3 import process_department_breakdown
from src.task4 import process_top_3_performers
from src.task5 import process_top_3_most_sold
from src.task6 import process_best_salesperson
from src.extra_insight_one import extra_insight_one
from src.extra_insight_two import extra_insight_two

def setup_logging(log_file='logs/data_processing.log') -> logging.Logger:
    """
    Set up logging with a rotating file handler.

    :param log_file: The file path for the log file.
    :return: Configured logger instance.
    """
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Check if the logger already has handlers configured
    if not logger.hasHandlers():
        handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
        handler.setLevel(logging.INFO)
    
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
    
        logger.addHandler(handler)
    
    return logger

def initialize_spark(app_name="DataProcessing") -> SparkSession:
    """
    Initialize a Spark session.

    :param app_name: The name of the Spark application.
    :return: SparkSession object.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark: SparkSession, file_path: str, logger: logging.Logger) -> SparkSession:
    """
    Load a CSV file into a Spark DataFrame.

    :param spark: The Spark session.
    :param file_path: The path to the CSV file.
    :param logger: The logger for logging activities.
    :return: DataFrame containing the loaded data.
    """
    logger.info(f"Loading data from {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def main():
    # Initialize logging
    logger = setup_logging()
    
    # Initialize Spark session
    spark = initialize_spark()

    # Load datasets
    df1 = load_data(spark, "data/dataset_one.csv", logger)
    df2 = load_data(spark, "data/dataset_two.csv", logger)
    df3 = load_data(spark, "data/dataset_three.csv", logger)
    
    # Execute each task
    logger.info("Starting IT data processing...")
    process_it_data(df1, df2)
    
    logger.info("Starting Marketing address information processing...")
    process_marketing_address_info(df1, df2)
    
    logger.info("Starting Department breakdown processing...")
    process_department_breakdown(df1, df2)
    
    logger.info("Starting Top 3 performers processing...")
    process_top_3_performers(df1, df2)
    
    logger.info("Starting Top 3 most sold products processing in Netherlands...")
    process_top_3_most_sold(df1, df2, df3)
    
    logger.info("Starting Best salesperson processing...")
    process_best_salesperson(df1, df2, df3)
    
    logger.info("Running Extra Insight One...")
    #extra_insight_one(df1, df2, df3)
    
    logger.info("Running Extra Insight Two...")
    extra_insight_two(df1, df2, df3)

    logger.info("All tasks completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()