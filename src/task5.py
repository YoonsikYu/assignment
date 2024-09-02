import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, desc, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import os

logger = logging.getLogger(__name__)

def process_top_3_most_sold(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    Process the top 3 most sold products per department in the Netherlands by joining three datasets,
    filtering for the Netherlands, aggregating sales data, and saving the results to a CSV file.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    df3: The third input DataFrame.
    return: A DataFrame containing the top 3 most sold products per department in the Netherlands.
    """
    logger.info("Joining datasets on 'id' and 'caller_id' columns...")
    df = df1.join(df2, on='id', how='inner')
    df_join = df3.join(df, df3.caller_id == df.id, 'left')

    logger.info("Filtering data for the Netherlands...")
    NL_df = df_join.filter(col('country') == 'Netherlands')

    logger.info("Aggregating total quantity sold per area and product in the Netherlands...")
    NL = NL_df.groupBy('area', 'product_sold', 'country').agg(sum('quantity').alias('total_quantity'))

    logger.info("Ranking products by total quantity sold within each area...")
    window_NL_sales = Window.partitionBy("area").orderBy(desc("total_quantity"))
    df_NLsales = NL.withColumn("NL_sales_rank", row_number().over(window_NL_sales).cast(IntegerType())).filter(col("NL_sales_rank") <= 3)

    output_path = os.path.join('output', 'top_3_most_sold_per_department_netherlands')
    logger.info(f"Writing top 3 most sold products results to {output_path}...")
    df_NLsales.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Top 3 most sold products processing completed successfully.")
    return df_NLsales