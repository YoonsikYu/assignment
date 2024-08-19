from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TASK7") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')
csv_file_path3 = os.path.join('data', 'dataset_three.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)
df3 = spark.read.csv(csv_file_path3, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')
df_join = df3.join(df, df3.caller_id == df.id, 'left')

dftest1 = df_join.groupBy('product_sold','country').agg(sum('quantity').alias('total_quantity'),sum('sales_amount').alias('total_sales_amount'))
dftest1 = dftest1.withColumn('product_price', round(col('total_sales_amount')/col('total_quantity'),2)) 

df_pivot = dftest1 .groupBy(col("product_sold").alias('product')) \
             .pivot("country") \
             .agg({"product_price": "first"})

output_path = os.path.join('output', 'extra_insight_one')

df_pivot.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()

