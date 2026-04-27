from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import settings



DBT_ROOT_DIR = F"{settings.DAGS_FOLDER}/ecommerce_dbt"

@dag(
    dag_id="dag_pipeline",
    default_args={
        "owner": "data-engineering team",
        "depends_on_past": False,
        'retries':2,
        'retry_delay': timedelta(seconds=15)
    },
    schedule=timedelta(hours=6),
    start_date=datetime(2026, 4, 25),
    catchup=False,
    tags=["dbt","medaillon","ecommerce","analytics"])
def dag_pipeline():
    @task
    def task_pipeline():
       import logging
       logger = logging.getLogger(__name__)
       logger.info("Starting pipeline...")

       pipeline_meatdata = {
           'pipeline_start_time': datetime.now().isoformat(),
           'dbt_root_dir': DBT_ROOT_DIR,
           'pipeline_id': f'dag_pipeline_{datetime.now().strftime("%Y%m%d%H%M%S")}',
           'environment': 'production',
       }

       logger.info(f"Pipeline metadata: {pipeline_meatdata}")

       return pipeline_meatdata
    


    @task
    def seed_bronze(pipeline_meatdata):
        import logging
        from dags.operators.dbt_operator import DbtOperator
        logger = logging.getLogger(__name__)
        logger.info("Starting seed_bronze task...")

        try:
            import sqlalchemy
            from sqlalchemy import create_engine, text

            engine = sqlalchemy.create_engine('trino://trino@trino-coordinator:8080/iceberg/bronze')
            with engine.connect() as conn:
                result = conn.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                row_count = result.scalar()

                if row_count and row_count > 0:
                    logger.info(f"raw_customer_events table already has {row_count} rows. Skipping seeding.")
                    return {
                        'status': 'skipped',
                        'layer': 'bronze_seed',
                        'pipeline_id': pipeline_meatdata['pipeline_id'],
                        'timestamp': datetime.now().isoformat(),
                        'message': f"raw_customer_events already has {row_count} rows."

                    }
        except Exception as e:
            logger.error(f"Table don't exist or error connecting to Trino: {e}. Proceeding with seeding.")

        operator = DbtOperator(
            task_id = 'seed_bronze_data_internal',
            dbt_root_dir = DBT_ROOT_DIR,
            dbt_command = 'seed',
            full_refresh=True
        )