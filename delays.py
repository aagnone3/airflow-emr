# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an Apache Airflow dag for an AWS EMR pipeline using Apache Spark to ETL disparate
data sources into a parquet data mart.
"""
import json
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'retries': 2,
    'start_date': airflow.utils.dates.days_ago(1),
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': True
}

with DAG(
    dag_id='flight_delays_emr',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval='@once',
) as dag:

    start_operator = DummyOperator(task_id='begin_execution', dag=dag)
    end_operator = DummyOperator(task_id='stop_execution', dag=dag)

    with open('emr_job_flow.json', 'r') as fp:
        job_flow = json.load(fp)
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=job_flow,
        aws_conn_id='aws_credentials',
        emr_conn_id='emr_default'
    )

    job_sensor = EmrJobFlowSensor(
        task_id='check_job_flow',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_credentials'
    )

    # define the DAG structure, in terms of the created operators
    start_operator >> cluster_creator >> job_sensor >> end_operator
