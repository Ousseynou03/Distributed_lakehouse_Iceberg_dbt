from datetime import datetime, timezone, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from pipeline.extractors.fakestore_extractor import (
    FakestoreExtractor,
    FakestoreExtractorError,
)
from pipeline.loaders.minio_loader import (
    MinioLoader,
    MinioLoaderError,
)


# ─── Constantes ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
MINIO_USER     = Variable.get("MINIO_USER",     default_var="minioadmin")
MINIO_PASSWORD = Variable.get("MINIO_PASSWORD", default_var="minioadmin")


# ─── DAG ─────────────────────────────────────────────────────────────────────

default_args = {
    "owner"           : "data-engineering",
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
}

@dag(
    dag_id            = "fakestore_bronze_ingestion",
    description       = "Ingestion brute Fake Store API → MinIO Bronze",
    schedule          = "0 * * * *",   # toutes les heures
    start_date        = datetime(2026, 4, 13, tzinfo=timezone.utc),
    catchup           = False,
    default_args      = default_args,
    tags              = ["bronze", "fakestore", "ingestion"],
)
def fakestore_bronze_ingestion():

    # ─── Task 1 : extraction ─────────────────────────────────────────────────

    @task(task_id="extract_products")
    def extract_products() -> list[dict]:
        return FakestoreExtractor().get_products()

    @task(task_id="extract_carts")
    def extract_carts() -> list[dict]:
        return FakestoreExtractor().get_carts()

    @task(task_id="extract_users")
    def extract_users() -> list[dict]:
        return FakestoreExtractor().get_users()

    # ─── Task 2 : chargement ─────────────────────────────────────────────────

    @task(task_id="load_products")
    def load_products(data: list[dict]) -> str:
        loader = MinioLoader(
            endpoint_url = MINIO_ENDPOINT,
            access_key   = MINIO_USER,
            secret_key   = MINIO_PASSWORD,
        )
        return loader.write("products", data)

    @task(task_id="load_carts")
    def load_carts(data: list[dict]) -> str:
        loader = MinioLoader(
            endpoint_url = MINIO_ENDPOINT,
            access_key   = MINIO_USER,
            secret_key   = MINIO_PASSWORD,
        )
        return loader.write("carts", data)

    @task(task_id="load_users")
    def load_users(data: list[dict]) -> str:
        loader = MinioLoader(
            endpoint_url = MINIO_ENDPOINT,
            access_key   = MINIO_USER,
            secret_key   = MINIO_PASSWORD,
        )
        return loader.write("users", data)

    # ─── Task 3 : résumé ─────────────────────────────────────────────────────

    @task(task_id="log_summary")
    def log_summary(path_products: str, path_carts: str, path_users: str) -> None:
        import logging
        logger = logging.getLogger(__name__)
        logger.info("=== Ingestion Bronze terminée ===")
        logger.info(f"  products → {path_products}")
        logger.info(f"  carts    → {path_carts}")
        logger.info(f"  users    → {path_users}")

    # ─── Orchestration ───────────────────────────────────────────────────────

    products_data = extract_products()
    carts_data    = extract_carts()
    users_data    = extract_users()

    path_products = load_products(products_data)
    path_carts    = load_carts(carts_data)
    path_users    = load_users(users_data)

    log_summary(path_products, path_carts, path_users)


fakestore_bronze_ingestion()