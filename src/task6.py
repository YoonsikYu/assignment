from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TASK6") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')
csv_file_path3 = os.path.join('data', 'dataset_three.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)
df3 = spark.read.csv(csv_file_path3, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')
df_join = df3.join(df, df3.caller_id == df.id, 'left')

df5 = df_join.groupBy('country','caller_id','name','address').agg(sum('quantity').alias('total_quantity'),sum('sales_amount').alias('total_sales_amount'))
window_global_sales = Window.partitionBy("country").orderBy(desc("total_sales_amount"))

df_global_sales = df5.withColumn("global_sales_rank", row_number().over(window_global_sales)).filter(col("global_sales_rank") == 1)

output_path = os.path.join('output', 'best_salesperson')

df_global_sales.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()