import json
import logging
from datetime import datetime, timezone
from io import BytesIO

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


# ─── Exceptions custom ───────────────────────────────────────────────────────

class MinioLoaderError(Exception):
    """Erreur de base pour le MinioLoader."""
    pass

class MinioConnectionError(MinioLoaderError):
    """Impossible de se connecter à MinIO."""
    pass

class MinioWriteError(MinioLoaderError):
    """Erreur lors de l'écriture dans MinIO."""
    pass

class MinioBucketNotFoundError(MinioLoaderError):
    """Le bucket cible n'existe pas."""
    pass


# ─── Loader ──────────────────────────────────────────────────────────────────

class MinioLoader:

    BUCKET = "lakehouse"

    def __init__(
        self,
        endpoint_url : str = "http://localhost:9000",
        access_key   : str = "minioadmin",
        secret_key   : str = "minioadmin",
        region       : str = "us-east-1",
    ):
        logger.info(f"Connexion à MinIO — {endpoint_url}")
        try:
            self.client = boto3.client(
                "s3",
                endpoint_url          = endpoint_url,
                aws_access_key_id     = access_key,
                aws_secret_access_key = secret_key,
                region_name           = region,
            )
            self._check_bucket()

        except (BotoCoreError, ClientError) as e:
            raise MinioConnectionError(f"Impossible de se connecter à MinIO : {e}")

    # ─── Vérification bucket ─────────────────────────────────────────────────

    def _check_bucket(self) -> None:
        """Vérifie que le bucket cible existe."""
        try:
            self.client.head_bucket(Bucket=self.BUCKET)
            logger.info(f"Bucket '{self.BUCKET}' trouvé.")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchBucket"):
                raise MinioBucketNotFoundError(
                    f"Le bucket '{self.BUCKET}' n'existe pas. "
                    "Vérifiez que minio-init s'est bien exécuté."
                )
            raise MinioConnectionError(f"Erreur lors de la vérification du bucket : {e}")

    # ─── Construction du path ─────────────────────────────────────────────────

    @staticmethod
    def _build_path(entity: str, ingested_at: datetime) -> str:
        """
        Construit le chemin de stockage Hive partitionné.

        Exemple :
            bronze/products/year=2026/month=04/day=13/hour=10/raw.json
        """
        return (
            f"bronze/{entity}/"
            f"year={ingested_at.strftime('%Y')}/"
            f"month={ingested_at.strftime('%m')}/"
            f"day={ingested_at.strftime('%d')}/"
            f"hour={ingested_at.strftime('%H')}/"
            f"raw.json"
        )

    # ─── Sérialisation ────────────────────────────────────────────────────────

    @staticmethod
    def _serialize(data: list[dict], ingested_at: datetime) -> bytes:
        """
        Enveloppe la donnée brute avec des métadonnées d'ingestion.

        Structure finale stockée :
        {
            "ingested_at" : "2026-04-13T10:00:00",
            "record_count": 20,
            "source"      : "fakestoreapi.com",
            "data"        : [ ... ]
        }
        """
        payload = {
            "ingested_at" : ingested_at.isoformat(),
            "record_count": len(data),
            "source"      : "fakestoreapi.com",
            "data"        : data,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    # ─── Écriture ─────────────────────────────────────────────────────────────

    def write(self, entity: str, data: list[dict]) -> str:
        """
        Écrit la donnée brute dans MinIO.

        Args:
            entity : nom de l'entité (products, carts, users)
            data   : liste de dictionnaires retournée par l'extracteur

        Returns:
            Le chemin complet du fichier écrit dans MinIO.
        """
        if not data:
            raise MinioWriteError(f"Données vides pour l'entité '{entity}', écriture annulée.")

        ingested_at = datetime.now(timezone.utc)
        path        = self._build_path(entity, ingested_at)
        body        = self._serialize(data, ingested_at)

        logger.info(f"Écriture de {len(data)} enregistrements → s3://{self.BUCKET}/{path}")

        try:
            self.client.put_object(
                Bucket      = self.BUCKET,
                Key         = path,
                Body        = BytesIO(body),
                ContentType = "application/json",
            )
        except ClientError as e:
            raise MinioWriteError(
                f"Échec de l'écriture pour '{entity}' → s3://{self.BUCKET}/{path} : {e}"
            )

        logger.info(f"Écriture réussie → s3://{self.BUCKET}/{path}")
        return f"s3://{self.BUCKET}/{path}"

    # ─── Écriture complète (les 3 entités) ───────────────────────────────────

    def write_all(self, extracted: dict[str, list[dict]]) -> dict[str, str]:
        """
        Écrit les 3 entités d'un coup.

        Args:
            extracted : dict retourné par FakestoreExtractor.get_all()

        Returns:
            Dict des chemins écrits par entité.
        """
        logger.info("=== Début écriture Bronze ===")
        paths = {}

        for entity, data in extracted.items():
            paths[entity] = self.write(entity, data)

        logger.info(f"=== Écriture Bronze terminée — {len(paths)} entités écrites ===")
        return paths

if __name__ == "__main__":
    import json

    # Données fictives pour tester
    fake_data = {
        "products": [{"id": 1, "title": "Test Product", "price": 9.99}],
        "carts": [{"id": 1, "userId": 1, "products": [{"productId": 1, "quantity": 2}]}],
        "users": [{"id": 1, "email": "test@test.com", "username": "testuser"}],
    }

    loader = MinioLoader(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )

    paths = loader.write_all(fake_data)

    print("\n=== Fichiers écrits dans MinIO ===")
    for entity, path in paths.items():
        print(f"  {entity} → {path}")