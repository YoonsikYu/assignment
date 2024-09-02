import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from src.task4 import process_top_3_performers

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("task4_test") \
        .getOrCreate()

def test_process_top_3_performers(spark):
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

    # Expected output DataFrame
    expected_df = spark.createDataFrame([
        ("708", "Finance", "Rosa de Jager", "Fabianring 277, 3274 DP, Gastel", 52, "67.31", "99827.12"),
        ("910", "Finance", "Dylano van Enschot-Meyer", "2847 LC, Westwoud", 88, "69.32", "99795.03"),
        ("864", "Finance", "Ceylin van Voorst", "Fayweg 82, 7959 WD, Middenmeer", 100, "95.0", "67153.61")
    ], ["id", "area", "name", "address", "calls_made", "call_success_rate", "sales_amount"])

    # Run the function under test
    result_df = process_top_3_performers(df1, df2)
       
    # Use chispa to compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_nullable=True)