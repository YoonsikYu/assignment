import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from src.extra_insight_one import extra_insight_one

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("extra_insight_test") \
        .getOrCreate()

def test_extra_insight_one(spark):
    # Create example input data
    df1_test = spark.createDataFrame([
        (1, "Marketing", 41, 21),
        (2, "Marketing", 26, 15),
        (3, "IT", 22, 12),
        (4, "IT", 23, 34),
        (5, "HR", 34, 21),
        (6, "HR", 21, 34),
        (7, "Games", 82, 34),
        (8, "Games", 34, 23),
        (9, "Finance", 11, 34)
    ], ["id", "area", "calls_made", "calls_successful"])

    df2_test = spark.createDataFrame([
        (1, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", 69087.89),
        (2, "Rosa Kuipers", "Jetlaan 816, 8779 EM, Holwierde", 37606.23),
        (3, "Vincent Mathurin", "4133HB", 44933.12),
        (4, "Sam", "1234df", 22342.13),
        (5, "Sara", "3423ds", 23413.32),
        (6, "Tom", "2341sd", 12342.31),
        (7, "JP", "2123ds", 34212.36),
        (8, "WP", "1255fd", 234029.21),
        (9, "EE", "4442df", 237744.21)
    ], ["id", "name", "address", "sales_amount"])

    df3_test = spark.createDataFrame([
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

    # Corrected expected DataFrame
    expected_df = spark.createDataFrame([
        ("Sign", 352.64, 35, 1635.05, 23, 1006.25, 34),
        ("Headset", 6686.55, 35, 21613.11, 11, 44933.12, 1),
        ("Banner", 1381.76, 50, 698.19, 32, 688.63, 34)
    ], ["product_sold", "Belgium_product_price", "Belgium_total_quantity_sold", "Germany_product_price", "Germany_total_quantity_sold", "Netherlands_product_price", "Netherlands_total_quantity_sold"])

    # Run the function under test
    result_df = extra_insight_one(df1_test, df2_test, df3_test)

    # Use chispa to compare DataFrames
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
