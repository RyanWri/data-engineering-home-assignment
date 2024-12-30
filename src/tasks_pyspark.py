import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import load_and_prepare_data_spark


def calc_question_1_spark(df):
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn(
        "daily_return",
        (F.col("close") - F.lag("close").over(window_spec))
        / F.lag("close").over(window_spec),
    )

    df = df.filter(F.col("daily_return").isNotNull())

    avg_daily_return = (
        df.groupBy("ticker", "Date")
        .agg(F.mean("daily_return").alias("average_daily_return"))
        .withColumn("average_daily_return", F.round("average_daily_return", 5))
    )

    result = avg_daily_return.groupBy("Date").agg(
        F.collect_list(F.struct("ticker", "average_daily_return")).alias(
            "average_return"
        )
    )

    return result


def calc_question_2_spark(df):
    df = df.withColumn("daily_worth", F.col("close") / F.col("volume"))
    average_daily_worth = df.groupBy("ticker").agg(F.mean("daily_worth").alias("value"))
    highest_worth = average_daily_worth.orderBy(F.col("value").desc()).limit(1)
    return highest_worth


def calc_question_3_spark(df):
    trading_days = 252
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn(
        "daily_return",
        (F.col("close") - F.lag("close").over(window_spec))
        / F.lag("close").over(window_spec),
    )
    df = df.filter(F.col("daily_return").isNotNull())
    volatility = df.groupBy("ticker").agg(
        (F.stddev("daily_return") * F.lit(trading_days**0.5)).alias(
            "standard_deviation"
        )
    )
    most_volatile = (
        volatility.orderBy(F.col("standard_deviation").desc())
        .select("ticker", "standard_deviation")
        .limit(1)
    )
    return most_volatile


def calc_question_4_spark(df):
    # Step 1: Calculate the 30-day return
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn(
        "30_day_return",
        (
            (F.col("close") - F.lag("close", 30).over(window_spec))
            / F.lag("close", 30).over(window_spec)
        )
        * 100,
    )

    # Step 2: Remove rows with null 30-day return
    df = df.filter(F.col("30_day_return").isNotNull())

    # Step 3: Extract the top 3 rows with the highest 30-day return
    top_3_returns = (
        df.orderBy(F.col("30_day_return").desc())  # Order by descending 30-day return
        .select("ticker", "Date")  # Select only ticker and Date
        .limit(3)  # Limit to top 3
    )

    return top_3_returns


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("rw_vilabs")
        .master("local[*]")  # Use all available cores
        .config("spark.driver.bindAddress", "127.0.0.1")  # Force binding to localhost
        .config("spark.driver.port", "4040")  # Optionally set a specific driver port
        .getOrCreate()
    )

    # load spark and clean data
    csv_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "stocks_data.csv"
    )
    data = load_and_prepare_data_spark(spark, csv_file_path)

    # question 1
    average_daily_return = calc_question_1_spark(data)
    average_daily_return.show(truncate=False)

    # question 2
    highest_worth_stock = calc_question_2_spark(data)
    highest_worth_stock.show(truncate=False)

    # question 3
    most_volatile_stock = calc_question_3_spark(data)
    most_volatile_stock.show(truncate=False)

    # question 4
    top_3_returns = calc_question_4_spark(data)
    top_3_returns.show(truncate=False)
