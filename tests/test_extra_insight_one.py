import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from chispa import assert_df_equality
from src.extra_insight_one import extra_insight_one
from pyspark.sql.types import IntegerType

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("extra_insight_two") \
        .getOrCreate()

def test_extra_insight_one(spark):
    # Create example input data
    df1 = spark.createDataFrame([
        (1, "Marketing", 41, 21),
        (2, "Marketing", 26, 15),
        (3, "IT", 22, 12),
    ], ["id", "area", "calls_made", "calls_successful"])
    
    df2 = spark.createDataFrame([
        (1, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", 69087.89),
        (2, "Rosa Kuipers", "Jetlaan 816, 8779 EM, Holwierde", 37606.23),
        (3, "Vincent Mathurin", "4133HB", 44933.12),
        (4, "Sam", "1234df", 22342.13),
        (5, "Sara", "3423ds", 23413.32),
        (6, "Tom", "2341sd", 12342.31),
        (7, "JP", "2123ds", 34212.36),
        (8, "WP", "1255fd", 234029,21),
        (9, "EE", "4442df", 237744,21)
    ], ["id", "name", "address", "sales_amount"])

    df3 = spark.createDataFrame([
        (1, 1, "Verbruggen-Vermeulen CommV", "Anny Claessens", 45, "Belgium", "Banner", 50),
        (2, 2, "Hendrickx CV", "Lutgarde Van Loock", 41, "Germany", "Sign", 23),
        (3, 3, "Koninklijke Aelftrud van Wessex", "Mustafa Ehlert", 34, "Netherlands", "Headset", 1),
        (4, 4, "dd", "alek", 23, "Germany", "Banner", 32),
        (5, 5, "ee", "sdwe", 44, "Netherlands", "Banner", 34),
        (6, 6, "er", "weer", 36, "Belgium", "Sign", 35),
        (7, 7, "der", "wezd", 34, "Netherlands", "Sign", 34),
        (8, 8, "sdq", "werdza", 52, "Belgium", "Headset", 35),
        (9, 9, "sdfe", "werers", 22, "Germany", "Headset", 11)
    ], ["id", "caller_id", "company", "recipient", "age", "country", "product_sold", "quantity"])

    expected_df = spark.createDataFrame([
        ("Headset", 2790.97, 4290, 2571.58, 3870, 2574.8, 3706),
        ("Sign", 2526.46, 4680, 2695.61, 4306, 2806.17, 3921),
        ("Banner", 2657.67, 4450, 2740.35, 4264, 2894.26, 3910)
    ], ["product_sold", "Belgium_product_price", "Belgium_total_quantity_sold", "Germany_product_price", "Germany_total_quantity_sold", "Netherlands_product_price", "Netherlands_total_quantity_sold"])

    # Run the function under test
    result_df = extra_insight_one(df1, df2, df3)
    
    # Use chispa to compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_nullable=True)