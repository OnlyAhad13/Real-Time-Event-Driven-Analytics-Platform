"""
Real-Time Analytics Platform - Airflow DAG
===========================================
Daily ETL pipeline orchestration:
1. Trigger Spark batch job (Silver → Warehouse)
2. Run dbt models
3. Run dbt tests

Schedule: Daily at 2 AM UTC
Retries: 3 with 5-minute delays
SLA: Alert if job exceeds 1 hour
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSensorTimeout

# ============================================
# DAG Configuration
# ============================================

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email': ['analytics-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(hours=1),
}

# ============================================
# Environment Configuration
# ============================================

# Paths (adjust for your environment)
PROJECT_ROOT = '/opt/airflow/project'  # Mount point in Airflow container
DBT_PROJECT_DIR = f'{PROJECT_ROOT}/dbt'
DBT_PROFILES_DIR = f'{PROJECT_ROOT}/dbt'

# Spark configuration
SPARK_MASTER_CONTAINER = 'analytics-spark-master'
SPARK_SUBMIT_CMD = '/spark/bin/spark-submit'
SPARK_PACKAGES = 'org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0'

# S3A/MinIO configuration
S3A_CONFIGS = """
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.access.key=minio_admin \
--conf spark.hadoop.fs.s3a.secret.key=minio_password_change_me \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
"""

# ============================================
# Helper Functions
# ============================================

def get_yesterday_date():
    """Get yesterday's date in YYYY-MM-DD format."""
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback triggered when SLA is missed."""
    print(f"SLA MISSED for DAG: {dag.dag_id}")
    print(f"Tasks affected: {[t.task_id for t in task_list]}")
    # Add custom alerting logic here (Slack, PagerDuty, etc.)


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='analytics_pipeline',
    default_args=default_args,
    description='Daily ETL: Spark Batch → dbt Run → dbt Test',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['analytics', 'etl', 'daily'],
    sla_miss_callback=sla_miss_callback,
    doc_md=__doc__,
) as dag:
    
    # ========================================
    # Task: Start
    # ========================================
    start = EmptyOperator(
        task_id='start',
        doc='Pipeline start marker'
    )
    
    # ========================================
    # Task: Spark Batch Job
    # ========================================
    # Triggers the Spark batch loader to move Silver → Warehouse
    
    spark_batch_job = BashOperator(
        task_id='spark_batch_job',
        bash_command=f"""
            docker exec {SPARK_MASTER_CONTAINER} {SPARK_SUBMIT_CMD} \
                --packages {SPARK_PACKAGES} \
                {S3A_CONFIGS} \
                /opt/spark-apps/batch_loader.py --date {{{{ ds }}}}
        """,
        doc='Execute Spark batch job to load Silver data into Warehouse',
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    
    # ========================================
    # Task: dbt Run
    # ========================================
    # Runs all dbt models to transform warehouse data
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --profiles-dir {DBT_PROFILES_DIR} --target dev
        """,
        doc='Execute dbt run to update all models',
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    # ========================================
    # Task: dbt Test
    # ========================================
    # Runs dbt tests to validate data quality
    
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --profiles-dir {DBT_PROFILES_DIR} --target dev
        """,
        doc='Execute dbt test to validate data quality',
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    # ========================================
    # Task: dbt Docs Generate (Optional)
    # ========================================
    # Generates dbt documentation
    
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt docs generate --profiles-dir {DBT_PROFILES_DIR} --target dev
        """,
        doc='Generate dbt documentation',
        trigger_rule='all_success',
    )
    
    # ========================================
    # Task: Data Quality Check
    # ========================================
    # Custom data quality validation
    
    def check_data_quality(**context):
        """
        Custom data quality checks.
        Raises exception if critical checks fail.
        """
        execution_date = context['ds']
        print(f"Running data quality checks for {execution_date}")
        
        # Add custom checks here, e.g.:
        # - Row count thresholds
        # - Value range validations
        # - Cross-table consistency checks
        
        return True
    
    data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
        doc='Custom data quality validation',
    )
    
    # ========================================
    # Task: End
    # ========================================
    end = EmptyOperator(
        task_id='end',
        doc='Pipeline end marker',
        trigger_rule='all_done',
    )
    
    # ========================================
    # Task Dependencies
    # ========================================
    # Define the execution order
    
    start >> spark_batch_job >> dbt_run >> dbt_test >> data_quality_check >> dbt_docs >> end


# ============================================
# Alternative: Docker Compose Execution
# ============================================
# If running Airflow outside Docker, use docker compose exec:

"""
Alternative Spark task using docker compose:

spark_batch_job = BashOperator(
    task_id='spark_batch_job',
    bash_command='''
        cd /path/to/project && \
        docker compose exec -T spark-master /spark/bin/spark-submit \
            --packages org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.6.0 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            /opt/spark-apps/batch_loader.py --date {{ ds }}
    ''',
)
"""
