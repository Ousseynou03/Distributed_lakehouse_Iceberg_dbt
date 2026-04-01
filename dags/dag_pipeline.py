from airflow import settings
from airflow.decorators import dag, task

from datetime import timedelta, datetime

DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/ecommerce_dbt/"

@dag(
    dag_id="dag_pipeline",
    default_args={
        "owner": "data-engineering-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    schedule=timedelta(hours=6),
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["'dbt', 'medaillon', 'ecommerce', 'analitycs'"],
    max_active_runs=1,
)
def dag_pipeline():
    @task()
    def start_pipeline():
        import logging

        logger = logging.getLogger("__name__")
        logger.info("Starting pipeline")

        pipeline_metadata = {
            'pipeline_start_time' : datetime.now().isoformat(),
            'dbt_root_dir': DBT_ROOT_DIR,
            'pipeline_id': f'dag_pipeline_{datetime.now().strftime("%Y%m%d%H%M%S")}',
            'environement': 'production'
        }

        logger.info(f'starting pipeline with id: {pipeline_metadata["pipeline_id"]}')

        return pipeline_metadata

    @task()
    def seed_bronze(pipeline_metadata):
        import logging
        logger = logging.getLogger("__name__")
        logger.info("seeding Bronze...")

        try:
            import sqlalchemy
            from sqlalchemy import text
            engine = sqlalchemy.create_engine('trino//:trino@trino-coordinator:8080/iceberg/bronze')
            with engine.connect() as conn:
                result = conn.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                row_count = result.scalar()

                if row_count and row_count > 0:
                    logger.info(f"bronze already seeded with {row_count} rows, skipping seeding")
                    return {
                        'status' : 'skipped',
                        'layer': 'bronze',
                        'pipeline_id' : pipeline_metadata['pipeline_id'],
                        'timestamp' : datetime.now().isoformat(),
                        'message' : f'Tables already seeded with {row_count} rows',
                    }


        except Exception as e:
            logger.info(f"Tables don't exist or error occured: {e}, proceeding with seeding")