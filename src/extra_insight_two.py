import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum, desc, row_number
from pyspark.sql.window import Window
import os

logger = logging.getLogger(__name__)

def extra_insight_two(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    """
    Generate additional insight two by joining three datasets, categorizing data by age group,
    calculating sales by age group, and saving the results to a CSV file.

    df1: The first input DataFrame.
    df2: The second input DataFrame.
    df3: The third input DataFrame.
    return: A DataFrame containing the top 3 products sold by age group.
    """
    logger.info("Joining datasets on 'id' and 'caller_id' columns...")
    df = df1.join(df2, on='id', how='inner')
    df_join = df3.join(df, df3.caller_id == df.id, 'left')

    logger.info("Categorizing data by age group...")
    df_with_age_group = df_join.withColumn(
        "age_group",
        when(col("age").between(0, 20), "0-20")
        .when(col("age").between(21, 30), "21-30")
        .when(col("age").between(31, 40), "31-40")
        .when(col("age").between(41, 50), "41-50")
        .otherwise("50+")
    )

    logger.info("Calculating total sales by age group and product sold...")
    df_sales_by_age = df_with_age_group.groupBy("age_group", "product_sold") \
        .agg(sum("quantity").alias("total_quantity"))

    logger.info("Ranking products by total quantity sold within each age group...")
    window_age_sales = Window.partitionBy("age_group").orderBy(desc("total_quantity"))
    df_age = df_sales_by_age.withColumn("age_rank", row_number().over(window_age_sales)).filter(col("age_rank") <= 3)

    output_path = os.path.join('output', 'extra_insight_two')
    logger.info(f"Writing additional insight two results to {output_path}...")
    df_age.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    logger.info("Additional insight two processing completed successfully.")
    return df_age




