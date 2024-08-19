from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder \
    .appName("TASK3") \
    .getOrCreate()

csv_file_path1 = os.path.join('data', 'dataset_one.csv')
csv_file_path2 = os.path.join('data', 'dataset_two.csv')

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)

df = df1.join(df2, on='id', how = 'inner')

df2 = df.groupBy('area').agg(
    sum(col('calls_made')).alias('total_calls_made'),
    sum(col('calls_successful')).alias('total_calls_successful'),
    sum(col('sales_amount')).alias('total_sales_amount'),
)

def format_large_number(value):
    if isinstance(value, (int, float)):  # Ensure value is numeric
        if value >= 1e9:
            return f"{value / 1e9:.1f}B"
        elif value >= 1e6:
            return f"{value / 1e6:.1f}M"
        elif value >= 1e3:
            return f"{value / 1e3:.1f}K"
        else:
            return f"{value:.1f}"
    return None

format_large_number_udf = udf(format_large_number, StringType())

df_with_formatted_sales = df2.withColumn(
    'call_success_rate',
    concat(
        round(col('total_calls_successful') / col('total_calls_made') * 100, 2).cast('string'),
        lit('%')
    )
).withColumn(
    'total_sales_amount(Million)',
    format_large_number_udf(col('total_sales_amount'))
).drop('total_calls_successful', 'total_sales_amount')

output_path = os.path.join('output', 'department_breakdown')

df_with_formatted_sales.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

<<<<<<< HEAD
spark.stop()  
  
=======
spark.stop()   
>>>>>>> cc408e00bf0c753b96abbc1c45c87a8f29b30690
