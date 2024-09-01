import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from src.task2 import process_marketing_address_info

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("task2_test") \
        .getOrCreate()

def test_process_marketing_address_info(spark):
    # Create example input data
    df1 = spark.createDataFrame([
        (1, "Marketing", 41, 21),
        (2, "Marketing", 26, 15),
        (3, "IT", 22, 12),
    ], ["id", "area", "calls_made", "calls_sucessful"])
    
    df2 = spark.createDataFrame([
        (1, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", "69087"),
        (2, "Rosa Kuipers", "Jetlaan 816, 8779 EM, Holwierde", "37606"),
        (3, "Vincent Mathurin", "4133HB", "44933"),
    ], ["id", "name", "address", "sales_amount"])

    # Expected output DataFrame
    expected_df = spark.createDataFrame([
        (1, "Marketing", 41, 21, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", "69087", "1808 KR"),
        (2, "Marketing", 26, 15, "Rosa Kuipers", "Jetlaan 816, 8779 EM, Holwierde", "37606", "8779 EM")
    ], ["id", "area", "calls_made", "calls_sucessful", "name", "address", "sales_amount", "zipcode"])

    # Run the function under test
    result_df = process_marketing_address_info(df1, df2)
       
    # Use chispa to compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_nullable=True)