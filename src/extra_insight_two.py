from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TASK8") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')
csv_file_path3 = os.path.join('data', 'dataset_three.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)
df3 = spark.read.csv(csv_file_path3, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')
df_join = df3.join(df, df3.caller_id == df.id, 'left')

df_with_age_group = df_join.withColumn(
    "age_group",
    when(col("age").between(0, 20), "0-20")
    .when(col("age").between(21, 30), "21-30")
    .when(col("age").between(31, 40), "31-40")
    .when(col("age").between(41, 50), "41-50")
    .otherwise("50+")
)

df_sales_by_age = df_with_age_group.groupBy("age_group","product_sold") \
    .agg(sum("quantity").alias("total_quantity"))

window_age_sales = Window.partitionBy("age_group").orderBy(desc("total_quantity"))

df_age = df_sales_by_age.withColumn("age_rank", row_number().over(window_age_sales)).filter(col("age_rank") <= 3)

output_path = os.path.join('output', 'extra_insight_two')

df_age.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()



