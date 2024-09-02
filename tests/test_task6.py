import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from chispa import assert_df_equality
from src.task6 import process_best_salesperson
from pyspark.sql.types import IntegerType

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("task6_test") \
        .getOrCreate()

def test_process_best_salesperson(spark):
    # Create example input data
    df1_test = spark.createDataFrame([
        (1, "Marketing", 41, 21),
        (2, "Marketing", 26, 15),
        (3, "IT", 22, 12),
    ], ["id", "area", "calls_made", "calls_successful"])
    
    df2_test = spark.createDataFrame([
        (1, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", 69087.89),
        (2, "Rosa Kuipers", "Jetlaan 816, 8779 EM, Holwierde", 37606.23),
        (3, "Vincent Mathurin", "4133HB", 44933.12),
    ], ["id", "name", "address", "sales_amount"])

    df3_test = spark.createDataFrame([
        (1, 1, "Verbruggen-Vermeulen CommV", "Anny Claessens", 45, "Belgium", "Banner", 50),
        (2, 2, "Hendrickx CV", "Lutgarde Van Loock", 41, "Belgium", "Sign", 23),
        (3, 3, "Koninklijke Aelftrud van Wessex", "Mustafa Ehlert", 34, "Netherlands", "Headset", 1),
    ], ["id", "caller_id", "company", "recipient", "age", "country", "product_sold", "quantity"])

    expected_df = spark.createDataFrame([
        ("Belgium", 1, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", 50, 69087.89, 1),
        ("Netherlands", 3, "Vincent Mathurin", "4133HB", 1, 44933.12, 1)
    ], ["country", "caller_id", "name", "address", "total_quantity", "total_sales_amount", "global_sales_rank"]).withColumn("global_sales_rank", col("global_sales_rank").cast(IntegerType()))


    # Run the function under test
    result_df = process_best_salesperson(df1_test, df2_test, df3_test)
    
    # Use chispa to compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_nullable=True)