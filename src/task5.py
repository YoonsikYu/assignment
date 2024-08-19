from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TASK5") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')
csv_file_path3 = os.path.join('data', 'dataset_three.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)
df3 = spark.read.csv(csv_file_path3, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')
df_join = df3.join(df, df3.caller_id == df.id, 'left')
NL_df= df_join.filter(col('country')== 'Netherlands')

NL = NL_df.groupBy('area','product_sold','country').agg(sum('quantity').alias('total_quantity'))

window_NL_sales = Window.partitionBy("area").orderBy(desc("total_quantity"))

df_NLsales = NL.withColumn("NL_sales_rank", row_number().over(window_NL_sales)).filter(col("NL_sales_rank") <= 3)

output_path = os.path.join('output', 'top_3_most_sold_per_department_netherlands')

df_NLsales.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()