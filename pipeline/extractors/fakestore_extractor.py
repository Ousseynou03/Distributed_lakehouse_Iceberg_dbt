import time
import logging
import requests
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ─── Exceptions custom ───────────────────────────────────────────────────────

class FakestoreExtractorError(Exception):
    """Erreur de base pour le FakestoreExtractor."""
    pass

class FakestoreHTTPError(FakestoreExtractorError):
    """Erreur HTTP (4xx, 5xx)."""
    pass

class FakestoreEmptyResponseError(FakestoreExtractorError):
    """L'API a répondu 200 mais la donnée est vide."""
    pass

class FakestoreInvalidJSONError(FakestoreExtractorError):
    """La réponse n'est pas du JSON valide."""
    pass

class FakestoreMaxRetriesError(FakestoreExtractorError):
    """Nombre maximum de tentatives atteint."""
    pass


# ─── Extracteur ──────────────────────────────────────────────────────────────

class FakestoreExtractor:

    BASE_URL    = "https://fakestoreapi.com"
    TIMEOUT     = 10
    MAX_RETRIES = 3
    BACKOFF     = 2

    ENDPOINTS = {
        "products" : "/products",
        "carts"    : "/carts",
        "users"    : "/users",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept"    : "application/json",
            "User-Agent": "lakehouse-ingestion/1.0",
        })

    # ─── Méthode centrale de fetch avec retry ────────────────────────────────

    def _fetch(self, endpoint: str) -> Any:
        url     = f"{self.BASE_URL}{endpoint}"
        attempt = 0

        while attempt < self.MAX_RETRIES:
            attempt += 1
            wait = self.BACKOFF ** (attempt - 1)

            logger.info(f"Tentative {attempt}/{self.MAX_RETRIES} — GET {url}")

            try:
                response = self.session.get(url, timeout=self.TIMEOUT)

            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Erreur de connexion (tentative {attempt}) : {e}")
                self._wait_or_raise(attempt, wait, url)
                continue

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout après {self.TIMEOUT}s (tentative {attempt})")
                self._wait_or_raise(attempt, wait, url)
                continue

            except requests.exceptions.RequestException as e:
                logger.warning(f"Erreur réseau inattendue (tentative {attempt}) : {e}")
                self._wait_or_raise(attempt, wait, url)
                continue

            # ── Validation status HTTP ────────────────────────────────────────
            if response.status_code == 404:
                raise FakestoreHTTPError(f"Endpoint introuvable (404) : {url}")

            if response.status_code >= 500:
                logger.warning(f"Erreur serveur {response.status_code} (tentative {attempt})")
                self._wait_or_raise(attempt, wait, url)
                continue

            if response.status_code != 200:
                raise FakestoreHTTPError(
                    f"Status HTTP inattendu {response.status_code} : {url}"
                )

            # ── Validation JSON ───────────────────────────────────────────────
            try:
                data = response.json()
            except ValueError:
                raise FakestoreInvalidJSONError(
                    f"La réponse de {url} n'est pas du JSON valide."
                )

            # ── Validation contenu non vide ───────────────────────────────────
            if not data:
                raise FakestoreEmptyResponseError(
                    f"L'API a retourné une réponse vide pour : {url}"
                )

            logger.info(f"Succès — {len(data)} enregistrements récupérés depuis {url}")
            return data

        raise FakestoreMaxRetriesError(
            f"Echec après {self.MAX_RETRIES} tentatives pour : {url}"
        )

    def _wait_or_raise(self, attempt: int, wait: float, url: str) -> None:
        """Attend avant le prochain retry, ou lève une exception si max atteint."""
        if attempt < self.MAX_RETRIES:
            logger.info(f"Attente de {wait}s avant nouvelle tentative...")
            time.sleep(wait)
        else:
            raise FakestoreMaxRetriesError(
                f"Echec après {self.MAX_RETRIES} tentatives pour : {url}"
            )

    # ─── Méthodes publiques ───────────────────────────────────────────────────

    def get_products(self) -> list[dict]:
        logger.info("Extraction des produits...")
        return self._fetch(self.ENDPOINTS["products"])

    def get_carts(self) -> list[dict]:
        logger.info("Extraction des carts...")
        return self._fetch(self.ENDPOINTS["carts"])

    def get_users(self) -> list[dict]:
        logger.info("Extraction des users...")
        return self._fetch(self.ENDPOINTS["users"])

    def get_all(self) -> dict[str, list[dict]]:
        logger.info("=== Début extraction complète Fakestore ===")
        result = {
            "products" : self.get_products(),
            "carts"    : self.get_carts(),
            "users"    : self.get_users(),
        }
        total = sum(len(v) for v in result.values())
        logger.info(f"=== Extraction terminée — {total} enregistrements au total ===")
        return result