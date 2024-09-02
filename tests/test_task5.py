import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from src.task5 import process_top_3_most_sold

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("task5_test") \
        .getOrCreate()

def test_process_top_3_most_sold(spark):
    # Create example input data
    df1 = spark.createDataFrame([
        (1, "Marketing", 41, 21),
        (2, "Marketing", 26, 15),
        (3, "IT", 22, 12),
    ], ["id", "area", "calls_made", "calls_successful"])
    
    df2 = spark.createDataFrame([
        (1, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", "69087"),
        (2, "Rosa Kuipers", "Jetlaan 816, 8779 EM, Holwierde", "37606"),
        (3, "Vincent Mathurin", "4133HB", "44933"),
    ], ["id", "name", "address", "sales_amount"])

    df3 = spark.createDataFrame([
        (1, 40, "Verbruggen-Vermeulen CommV", "Anny Claessens", 45, "Belgium", "Banner", 50),
        (2, 17, "Hendrickx CV", "Lutgarde Van Loock", 41, "Belgium", "Sign", 23),
        (3, 15, "Koninklijke Aelftrud van Wessex", "Mustafa Ehlert", 34, "Netherlands", "Headset", 1),
    ], ["id", "caller_id", "company", "recipient", "age", "country", "product_sold", "quantity"])


    expected_df = spark.createDataFrame([
        ("Marketing", "Headset", "Netherlands", 1, 1)
    ], ["area", "product_sold", "country", "total_quantity", "NL_sales_rank"])
    
    # Run the function under test
    result_df = process_top_3_most_sold(df1, df2, df3)
    
    # Use chispa to compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_nullable=True)

