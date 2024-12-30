import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def load_and_prepare_data_spark(glue_context, file_path):
    spark = glue_context.spark_session
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.withColumn("Date", F.to_date("Date"))
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn("close", F.last("close", ignorenulls=True).over(window_spec))
    df = df.withColumn(
        "close",
        F.when(
            F.col("close").isNull(),
            F.first("close", ignorenulls=True).over(window_spec),
        ).otherwise(F.col("close")),
    )
    return df


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
    # Flatten average_return for CSV compatibility
    result = avg_daily_return.select("Date", "ticker", "average_daily_return")
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
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn(
        "30_day_return",
        (
            (F.col("close") - F.lag("close", 30).over(window_spec))
            / F.lag("close", 30).over(window_spec)
        )
        * 100,
    )
    df = df.filter(F.col("30_day_return").isNotNull())
    top_3_returns = (
        df.orderBy(F.col("30_day_return").desc()).select("ticker", "Date").limit(3)
    )
    return top_3_returns


# Glue-specific setup
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Read S3 bucket and input file path as parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_file_path", "output_base_path"])
input_file_path = args["input_file_path"]  # Path to input CSV in S3
output_base_path = args["output_base_path"]  # Base output S3 path

# Load and prepare data
data = load_and_prepare_data_spark(glue_context, input_file_path)

# Question 1
q1_results = calc_question_1_spark(data)
q1_results.write.mode("overwrite").csv(f"{output_base_path}/question_1", header=True)

# Question 2
q2_results = calc_question_2_spark(data)
q2_results.write.mode("overwrite").csv(f"{output_base_path}/question_2", header=True)

# Question 3
q3_results = calc_question_3_spark(data)
q3_results.write.mode("overwrite").csv(f"{output_base_path}/question_3", header=True)

# Question 4
q4_results = calc_question_4_spark(data)
q4_results.write.mode("overwrite").csv(f"{output_base_path}/question_4", header=True)
