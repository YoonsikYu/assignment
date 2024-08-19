from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder \
    .appName("TASK2") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')

zip_pattern = r'(\d{4} [A-Za-z]{2})'
df_with_zip = df.filter(col('area')=='Marketing').withColumn("zipcode", regexp_extract("ADDRESS", zip_pattern, 1))

output_path = os.path.join('output', 'marketing_address_info')

df_with_zip.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()   