import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col, round, first
import os

logger = logging.getLogger(__name__)

def extra_insight_one(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    Generate additional insight two by joining three datasets, categorizing data by age group,
    calculating sales by age group, and saving the results to a CSV file.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    df3: The third input DataFrame.
    return: A DataFrame containing the pivoted results.
    """
    logger.info("Joining datasets on 'id' and 'caller_id' columns...")
    df = df1.join(df2, on='id', how='inner')
    df_join = df3.join(df, df3.caller_id == df.id, 'left')

    logger.info("Aggregating total quantity and sales amount...")
    dftest1 = df_join.groupBy('product_sold', 'country').agg(
        sum('quantity').alias('total_quantity'),
        sum('sales_amount').alias('total_sales_amount')
    )
    
    logger.info("Calculating product price...")
    dftest1 = dftest1.withColumn('product_price', round(col('total_sales_amount') / col('total_quantity'), 2))

    logger.info("Aggregating total quantity sold and product price by product and country...")
    dftest1_with_quantity = dftest1.groupBy("product_sold", "country").agg(
        sum("total_quantity").alias("total_quantity_sold"),
        first("product_price").alias("product_price")
    )

    logger.info("Pivoting the data to show product prices and quantities sold by country...")
    df_pivot = dftest1_with_quantity.groupBy("product_sold").pivot("country").agg(
        first("product_price").alias("product_price"),
        first("total_quantity_sold").alias("total_quantity_sold")
    )

    output_path = os.path.join('output', 'extra_insight_one')
    logger.info(f"Writing additional insight one results to {output_path}...")
    df_pivot.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Additional insight one processing completed successfully.")
    return df_pivot