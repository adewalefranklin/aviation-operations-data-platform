from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import (
    EmrJobFlowSensor,
    EmrStepSensor,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# AWS and project configuration

AWS_REGION = "us-east-1"

S3_BUCKET = "aviation-operations-data-platform"
RAW_PREFIX = "raw/opensky/states/"

SPARK_SCRIPT = (
    "s3://aviation-operations-data-platform/"
    "scripts/emr/states_to_iceberg.py"
)

REJECTED_PATH = (
    "s3://aviation-operations-data-platform/"
    "data-quality/rejected/opensky/states/"
)

GLUE_DATABASE = "aviation_operations"
ICEBERG_TABLE = "opensky_flight_states_iceberg"

EMR_SUBNET_ID = "subnet-0c5d99ec8800cec7a"
EMR_SERVICE_ROLE = "EMR_DefaultRole_V2"
EMR_EC2_INSTANCE_PROFILE = "EMR_EC2_DefaultRole"


# Transient EMR cluster configuration

JOB_FLOW_OVERRIDES = {
    "Name": "aviation-opensky-iceberg-transient",
    "ReleaseLabel": "emr-7.13.0",
    "Applications": [
        {
            "Name": "Spark",
        },
    ],
    "LogUri": (
        "s3://aviation-operations-data-platform/"
        "logs/emr/"
    ),
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "Ec2SubnetId": EMR_SUBNET_ID,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": EMR_EC2_INSTANCE_PROFILE,
    "ServiceRole": EMR_SERVICE_ROLE,
    "VisibleToAllUsers": True,
    "AutoTerminationPolicy": {
        "IdleTimeout": 900,
    },
    "Tags": [
        {
            "Key": "for-use-with-amazon-emr-managed-policies",
            "Value": "true",
        },
        {
            "Key": "Project",
            "Value": "aviation-operations-data-platform",
        },
        {
            "Key": "ManagedBy",
            "Value": "airflow",
        },
    ],
}


# Validation functions

def validate_raw_s3_data() -> str:
    """
    Find and validate the latest raw OpenSky object.

    The exact S3 URI is returned through XCom so the EMR step
    processes only the newly ingested file.
    """

    s3_hook = AwsBaseHook(
        aws_conn_id=None,
        client_type="s3",
        region_name=AWS_REGION,
    )

    s3_client = s3_hook.get_conn()
    paginator = s3_client.get_paginator(
        "list_objects_v2"
    )

    latest_object: dict[str, Any] | None = None

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=RAW_PREFIX,
    ):
        for current_object in page.get(
            "Contents",
            [],
        ):
            if (
                latest_object is None
                or current_object["LastModified"]
                > latest_object["LastModified"]
            ):
                latest_object = current_object

    if latest_object is None:
        raise RuntimeError(
            "No raw OpenSky objects were found under "
            f"s3://{S3_BUCKET}/{RAW_PREFIX}"
        )

    object_key = latest_object["Key"]
    object_size = latest_object["Size"]

    if object_size <= 0:
        raise RuntimeError(
            "Raw OpenSky object is empty: "
            f"s3://{S3_BUCKET}/{object_key}"
        )

    response = s3_client.get_object(
        Bucket=S3_BUCKET,
        Key=object_key,
    )

    raw_body = response["Body"].read()

    try:
        payload = json.loads(
            raw_body.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(
            "Raw OpenSky object is not valid JSON: "
            f"s3://{S3_BUCKET}/{object_key}"
        ) from exc

    states = payload.get("states")

    if not isinstance(states, list):
        raise RuntimeError(
            "Raw OpenSky payload does not contain "
            "a valid 'states' list: "
            f"s3://{S3_BUCKET}/{object_key}"
        )

    if len(states) == 0:
        raise RuntimeError(
            "Raw OpenSky payload contains no aircraft states: "
            f"s3://{S3_BUCKET}/{object_key}"
        )

    return f"s3://{S3_BUCKET}/{object_key}"


def validate_iceberg_table() -> None:
    """
    Confirm that the Glue-backed table exists and is registered
    as an Apache Iceberg table.
    """

    glue_hook = AwsBaseHook(
        aws_conn_id=None,
        client_type="glue",
        region_name=AWS_REGION,
    )

    glue_client = glue_hook.get_conn()

    response = glue_client.get_table(
        DatabaseName=GLUE_DATABASE,
        Name=ICEBERG_TABLE,
    )

    table = response.get("Table")

    if table is None:
        raise RuntimeError(
            "Glue did not return the expected table: "
            f"{GLUE_DATABASE}.{ICEBERG_TABLE}"
        )

    returned_table_name = table.get("Name")

    if returned_table_name != ICEBERG_TABLE:
        raise RuntimeError(
            "Unexpected Glue table returned: "
            f"{returned_table_name}"
        )

    table_parameters = table.get(
        "Parameters",
        {},
    )

    table_type = table_parameters.get(
        "table_type",
        "",
    ).upper()

    if table_type != "ICEBERG":
        raise RuntimeError(
            "Glue table exists but is not registered "
            "as an Iceberg table: "
            f"{GLUE_DATABASE}.{ICEBERG_TABLE}"
        )


# EMR Spark step

SPARK_STEPS = [
    {
        "Name": "OpenSky raw JSON to Iceberg",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--conf",
                (
                    "spark.jars="
                    "/usr/share/aws/iceberg/lib/"
                    "iceberg-spark3-runtime.jar"
                ),
                SPARK_SCRIPT,
                "--raw-path",
                (
                    "{{ ti.xcom_pull("
                    "task_ids='validate_raw_s3_data') }}"
                ),
                "--rejected-path",
                REJECTED_PATH,
            ],
        },
    },
]


# Default Airflow task configuration

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# DAG

with DAG(
    dag_id="aviation_operations_pipeline",
    description=(
        "Ingest OpenSky data and process it into a "
        "Glue-backed Apache Iceberg table"
    ),
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=[
        "aviation",
        "opensky",
        "aws",
        "emr",
        "iceberg",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    run_opensky_ingestion = BashOperator(
        task_id="run_opensky_ingestion",
        bash_command="""
        set -e

        cd /opt/airflow/aviation

        python main.py
        """,
    )

    validate_raw_s3_data_task = PythonOperator(
        task_id="validate_raw_s3_data",
        python_callable=validate_raw_s3_data,
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=None,
        emr_conn_id=None,
        region_name=AWS_REGION,
    )

    wait_for_emr_cluster = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        target_states=[
            "WAITING",
        ],
        failed_states=[
            "TERMINATED",
            "TERMINATED_WITH_ERRORS",
        ],
        aws_conn_id=None,
        region_name=AWS_REGION,
        poke_interval=30,
        timeout=3600,
    )

    submit_iceberg_step = EmrAddStepsOperator(
        task_id="submit_iceberg_step",
        job_flow_id=create_emr_cluster.output,
        steps=SPARK_STEPS,
        aws_conn_id=None,
        region_name=AWS_REGION,
    )

    wait_for_iceberg_step = EmrStepSensor(
        task_id="wait_for_iceberg_step",
        job_flow_id=create_emr_cluster.output,
        step_id=(
            "{{ ti.xcom_pull("
            "task_ids='submit_iceberg_step')[0] }}"
        ),
        target_states=[
            "COMPLETED",
        ],
        failed_states=[
            "CANCELLED",
            "FAILED",
            "INTERRUPTED",
        ],
        aws_conn_id=None,
        region_name=AWS_REGION,
        poke_interval=30,
        timeout=7200,
    )

    validate_iceberg_table_task = PythonOperator(
        task_id="validate_iceberg_table",
        python_callable=validate_iceberg_table,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        aws_conn_id=None,
        region_name=AWS_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        start
        >> run_opensky_ingestion
        >> validate_raw_s3_data_task
        >> create_emr_cluster
        >> wait_for_emr_cluster
        >> submit_iceberg_step
        >> wait_for_iceberg_step
        >> validate_iceberg_table_task
        >> terminate_emr_cluster
        >> end
    )