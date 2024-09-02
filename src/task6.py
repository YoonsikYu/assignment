import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, desc, row_number
from pyspark.sql.window import Window
import os

logger = logging.getLogger(__name__)

def process_best_salesperson(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    Process the best salesperson per country by joining three datasets, aggregating sales data,
    ranking by total sales amount, and saving the results to a CSV file.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    df3: The third input DataFrame.
    return: A DataFrame containing the best salesperson per country.
    """
    logger.info("Joining datasets on 'id' and 'caller_id' columns...")
    df = df1.join(df2, on='id', how='inner')
    df_join = df3.join(df, df3.caller_id == df.id, 'left')

    logger.info("Aggregating total quantity and sales amount per country, caller, and address...")
    df5 = df_join.groupBy('country', 'caller_id', 'name', 'address').agg(
        sum('quantity').alias('total_quantity'),
        sum('sales_amount').alias('total_sales_amount')
    )

    logger.info("Ranking salespeople by total sales amount within each country...")
    window_global_sales = Window.partitionBy("country").orderBy(desc("total_sales_amount"))
    df_global_sales = df5.withColumn("global_sales_rank", row_number().over(window_global_sales)).filter(col("global_sales_rank") == 1)

    output_path = os.path.join('output', 'best_salesperson')
    logger.info(f"Writing best salesperson results to {output_path}...")
    df_global_sales.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Best salesperson processing completed successfully.")
    return df_global_sales