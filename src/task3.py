import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, round, concat, lit, udf
from pyspark.sql.types import StringType
import os

logger = logging.getLogger(__name__)

def format_large_number(value: float) -> str:
    """
    Format large numbers into a human-readable string with suffixes (K, M, B).
    
    param value: The numeric value to format.
    return: A formatted string representing the number with suffixes.
    """
    if value >= 1e9:
        return f"{value / 1e9:.1f}B"
    elif value >= 1e6:
        return f"{value / 1e6:.1f}M"
    elif value >= 1e3:
        return f"{value / 1e3:.1f}K"
    else:
        return f"{value:.1f}"

def process_department_breakdown(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Process department breakdown by joining two datasets, aggregating sales data, 
    and formatting the results. The results are then saved to a CSV file.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    return: A DataFrame containing the department breakdown.
    """
    logger.info("Joining datasets on 'id' column...")
    df = df1.join(df2, on='id', how='inner')

    logger.info("Aggregating data by department...")
    df2 = df.groupBy('area').agg(
        sum(col('calls_made')).alias('total_calls_made'),
        sum(col('calls_successful')).alias('total_calls_successful'),
        sum(col('sales_amount')).alias('total_sales_amount'),
    )

    logger.info("Formatting large numbers and calculating call success rate...")
    format_large_number_udf = udf(format_large_number, StringType())

    df_with_formatted_sales = df2.withColumn(
        'call_success_rate',
        concat(
            round(col('total_calls_successful') / col('total_calls_made') * 100, 2).cast('string'),
            lit('%')
        )
    ).withColumn(
        'total_sales_amount(Million)',
        format_large_number_udf(col('total_sales_amount'))
    ).drop('total_calls_successful', 'total_sales_amount')

    output_path = os.path.join('output', 'department_breakdown')
    logger.info(f"Writing department breakdown results to {output_path}...")
    df_with_formatted_sales.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Department breakdown processing completed successfully.")
    return df_with_formatted_sales