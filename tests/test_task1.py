import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from src.task1 import process_it_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("task1_test") \
        .getOrCreate()

def test_process_it_data(spark):
    # Create example input data
    df1 = spark.createDataFrame([
        (1, "IT", 34, 21),
        (2, "IT", 25, 20),
        (3, "Marketing", 22, 12),
    ], ["id", "area", "calls_made", "calls_sucessful"])
    
    df2 = spark.createDataFrame([
        (1, "Sep Cant-Vandenbergh", "2588VD", "57750"),
        (2, "Evie Godfrey van Alemannië-Smits", "1808KR", "69087"),
        (3, "Vincent Mathurin", "4133HB", "44933"),
    ], ["id", "name", "address", "sales_amount"])
    
    # Expected output DataFrame
    expected_df = spark.createDataFrame([
        (1, "IT", 34, 21, "Sep Cant-Vandenbergh", "2588VD", "57750"),
        (2, "IT", 25, 20, "Evie Godfrey van Alemannië-Smits", "1808KR", "69087")
    ], ["id", "area", "calls_made", "calls_sucessful", "name", "address", "sales_amount"])
    
    # Run the function under test
    result_df = process_it_data(df1, df2)
    
    # Sort DataFrames by all columns to ensure order is the same
    result_df_sorted = result_df.orderBy(result_df.columns)
    expected_df_sorted = expected_df.orderBy(expected_df.columns)
    
    # Use chispa to compare DataFrames
    assert_df_equality(result_df_sorted, expected_df_sorted, ignore_nullable=True)