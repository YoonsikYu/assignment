import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract
import os

logger = logging.getLogger(__name__)

def process_marketing_address_info(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Process marketing address information by joining two datasets, filtering for the Marketing department,
    extracting zip codes, and saving the results to CSV.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    return: A DataFrame containing marketing addresses with extracted zip codes.
    """
    logger.info("Joining datasets on 'id' column...")
    df = df1.join(df2, on='id', how='inner')

    zip_pattern = r'(\d{4} [A-Za-z]{2})'
    
    logger.info("Filtering Marketing department and extracting zip codes...")
    df_with_zip = df.filter(col('area') == 'Marketing').withColumn("zipcode", regexp_extract("address", zip_pattern, 1))

    output_path = os.path.join('output', 'marketing_address_info')
    logger.info(f"Writing results to {output_path}...")
    df_with_zip.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Marketing address information processing completed successfully.")
    return df_with_zip