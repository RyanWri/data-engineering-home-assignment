import boto3
import os
from dotenv import load_dotenv


def trigger_glue_job(input_file_path, output_base_path):
    glue_client = boto3.client(
        "glue",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

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


if __name__ == "__main__":
    load_dotenv()
    input_file_path = "s3://data-engineer-assignment-ranw/input/"
    output_base_path = "s3://data-engineer-assignment-ranw/results"
    trigger_glue_job(input_file_path, output_base_path)
