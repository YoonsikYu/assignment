import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, round, desc, row_number
from pyspark.sql.window import Window
import os

logger = logging.getLogger(__name__)

def process_top_3_performers(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Process the top 3 performers by joining two datasets, calculating call success rate,
    ranking by sales amount and call success rate, and saving the results to a CSV file.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    return: A DataFrame containing the top 3 performers.
    """
    logger.info("Joining datasets on 'id' column...")
    df = df1.join(df2, on='id', how='inner')

    logger.info("Calculating call success rate and filtering records with more than 50 calls made...")
    df = df.withColumn('call_success_rate', round(col('calls_successful') / col('calls_made') * 100, 2)).drop('calls_successful')
    df_filter = df.filter(col('calls_made') > 50)

    logger.info("Ranking performers by sales amount and call success rate...")
    window_spec_sales = Window.partitionBy("area").orderBy(desc("sales_amount"))
    window_spec_calls = Window.partitionBy("area").orderBy(desc("call_success_rate"))

    df_sales = df.withColumn("sales_rank", row_number().over(window_spec_sales)).filter(col("sales_rank") <= 2)
    df_calls = df_filter.withColumn("calls_rank", row_number().over(window_spec_calls)).filter(col("calls_rank") == 1)

    logger.info("Combining sales and calls rankings, and selecting relevant columns...")
    result_df = df_sales.union(df_calls).drop('sales_rank', 'calls_rank')

    result_df = result_df.select('id', 'area', 'name', 'address', 'calls_made', 'call_success_rate', 'sales_amount').orderBy('area')

    output_path = os.path.join('output', 'top_3')
    logger.info(f"Writing top 3 performers results to {output_path}...")
    result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Top 3 performers processing completed successfully.")
    return result_df