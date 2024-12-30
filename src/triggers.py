import boto3
import os
from dotenv import load_dotenv


def trigger_glue_job(glue_client, input_file_path, output_base_path):
    try:
        response = glue_client.start_job_run(
            JobName="data_engineer_assignment_ranw",
            Arguments={
                "--input_file_path": input_file_path,
                "--output_base_path": output_base_path,
            },
        )
        print(f"Job triggered successfully. JobRunId: {response['JobRunId']}")
    except Exception as e:
        print(f"Error triggering Glue job: {str(e)}")


def trigger_crawler(glue_client, crawler_name):
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response["Crawler"]["State"]
        print(f"Crawler '{crawler_name}' status: {status}")
    except Exception as e:
        print(f"Error checking status for crawler '{crawler_name}': {str(e)}")


if __name__ == "__main__":
    load_dotenv()
    glue_client = boto3.client(
        "glue",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

    for i in range(1, 5):
        crawler_name = f"question_{i}_crawler_ranw"
        trigger_crawler(glue_client, crawler_name)

    input_file_path = "s3://data-engineer-assignment-ranw/input/"
    output_base_path = "s3://data-engineer-assignment-ranw/results"
    trigger_glue_job(input_file_path, output_base_path)
