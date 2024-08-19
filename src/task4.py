from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TASK4") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')

df = df.withColumn('call_success_rate', round(col('calls_successful') / col('calls_made') * 100, 2)).drop('calls_successful')
df_filter = df.filter(col('calls_made')>50)

window_spec_sales = Window.partitionBy("area").orderBy(desc("sales_amount"))
window_spec_calls = Window.partitionBy("area").orderBy(desc("call_success_rate"))

df_sales = df.withColumn("sales_rank", row_number().over(window_spec_sales)).filter(col("sales_rank") <= 2)
df_calls = df_filter.withColumn("calls_rank", row_number().over(window_spec_calls)).filter(col("calls_rank") == 1)

result_df = df_sales.union(df_calls).drop('sales_rank', 'calls_rank')

result_df = result_df.select('id','area','name','address','calls_made','call_success_rate','sales_amount').orderBy('area')

output_path = os.path.join('output', 'top_3')

result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()