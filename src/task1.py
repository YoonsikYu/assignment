import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import os

logger = logging.getLogger(__name__)

def process_it_data(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Process IT data by joining two datasets, filtering for the IT department,
    ordering by sales amount, and saving the top 100 records to CSV.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    return: A DataFrame containing the top 100 IT department records.
    """
    logger.info("Joining datasets on 'id' column...")
    df = df1.join(df2, on='id', how='inner')

    logger.info("Filtering IT department and ordering by sales amount...")
    it_data = df.filter(col('area') == 'IT').orderBy(col('sales_amount').desc())

    logger.info("Selecting top 100 records...")
    it_data_top100 = it_data.limit(100)

    output_path = os.path.join('output', 'it_data')
    logger.info(f"Writing results to {output_path}...")
    it_data_top100.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("IT data processing completed successfully.")
    return it_data_top100   