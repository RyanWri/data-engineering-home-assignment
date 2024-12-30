import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from io import StringIO
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()


def load_and_prepare_data_spark(spark, file_path):
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


def load_and_prepare_data_pandas(csv_file_path):
    data = pd.read_csv(csv_file_path)
    data["Date"] = pd.to_datetime(data["Date"])
    data = data.sort_values(by=["ticker", "Date"])
    data["close"] = data["close"].fillna(method="ffill").fillna(method="bfill")
    return data


def save_to_csv(df, output_file_path):
    df.to_csv(output_file_path, index=False)


def save_to_s3(df, bucket_name, s3_filename):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=s3_filename, Body=csv_buffer.getvalue())


def ensure_s3_bucket_exists(bucket_name):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Bucket {bucket_name} does not exist. Creating...")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": os.getenv("AWS_REGION")},
        )
        print(f"Bucket {bucket_name} created successfully.")
