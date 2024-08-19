from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("TASK1") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')
IT_df = df.filter(col('area')=='IT').orderBy(col('sales_amount').desc()).limit(100)

output_path = os.path.join('output', 'it_data')

IT_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop() 