# Step 1: Secure the Connection
    # Just like we did with the Postgres database, we never hardcode Databricks passwords in our script.

    # You go to your Databricks Workspace and generate a Personal Access Token (PAT).

    # You go to the Airflow UI -> Connections and create a new connection.

    # Set the Connection Type to Databricks.

    # Enter your Databricks Workspace URL in the Host field, and your PAT token in the Password field. Name the connection (e.g., databricks_default).
    
    from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'platform_team',
    'retries': 1
}

with DAG(
    dag_id='databricks_daily_processing',
    default_args=default_args,
    start_date=datetime(2026, 10, 24),
    schedule_interval='@daily',
    catchup=False
) as dag:

    start_task = DummyOperator(task_id='start')

    # Define the compute specifications for Databricks
    # We are asking Databricks to spin up a temporary "Job Cluster"
    cluster_config = {
        'spark_version': '13.3.x-scala2.12',
        'node_type_id': 'i3.xlarge', # The type of EC2 instance to use
        'num_workers': 2             # The number of worker nodes
    }

    # Define which notebook to run
    notebook_task_config = {
        'notebook_path': '/Users/c.guha@mycompany.com/premium_customers_job',
    }

    # The Operator that makes the API call
    run_databricks_notebook = DatabricksSubmitRunOperator(
        task_id='run_pyspark_job',
        databricks_conn_id='databricks_default', # Refers to the connection we made in the UI
        new_cluster=cluster_config,
        notebook_task=notebook_task_config,
        # You can pass dynamic Airflow variables directly into the Databricks notebook!
        notebook_params={'execution_date': '{{ ds }}'} 
    )

    end_task = DummyOperator(task_id='end')

    start_task >> run_databricks_notebook >> end_task
    
    