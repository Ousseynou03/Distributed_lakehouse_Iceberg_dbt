import pytest
from unittest.mock import MagicMock, patch
from requests.exceptions import ConnectionError, Timeout

from pipeline.extractors.fakestore_extractor import (
    FakestoreExtractor,
    FakestoreHTTPError,
    FakestoreEmptyResponseError,
    FakestoreInvalidJSONError,
    FakestoreMaxRetriesError,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def extractor():
    return FakestoreExtractor()

def make_response(status_code: int = 200, json_data=None, raise_json=False):
    """Helper pour construire une fausse réponse requests."""
    mock = MagicMock()
    mock.status_code = status_code
    if raise_json:
        mock.json.side_effect = ValueError("not json")
    else:
        mock.json.return_value = json_data
    return mock


# ─── Tests nominaux ──────────────────────────────────────────────────────────

class TestFetchSuccess:

    def test_get_products_retourne_une_liste(self, extractor):
        fake_data = [{"id": 1, "title": "Product A"}]
        with patch.object(extractor.session, "get", return_value=make_response(200, fake_data)):
            result = extractor.get_products()
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["title"] == "Product A"

    def test_get_carts_retourne_une_liste(self, extractor):
        fake_data = [{"id": 1, "userId": 2, "products": []}]
        with patch.object(extractor.session, "get", return_value=make_response(200, fake_data)):
            result = extractor.get_carts()
        assert isinstance(result, list)

    def test_get_users_retourne_une_liste(self, extractor):
        fake_data = [{"id": 1, "email": "test@test.com"}]
        with patch.object(extractor.session, "get", return_value=make_response(200, fake_data)):
            result = extractor.get_users()
        assert isinstance(result, list)

    def test_get_all_retourne_les_3_cles(self, extractor):
        fake_products = [{"id": 1}]
        fake_carts    = [{"id": 1}]
        fake_users    = [{"id": 1}]
        responses = [
            make_response(200, fake_products),
            make_response(200, fake_carts),
            make_response(200, fake_users),
        ]
        with patch.object(extractor.session, "get", side_effect=responses):
            result = extractor.get_all()
        assert set(result.keys()) == {"products", "carts", "users"}


# ─── Tests erreurs HTTP ───────────────────────────────────────────────────────

class TestHTTPErrors:

    def test_404_leve_http_error(self, extractor):
        with patch.object(extractor.session, "get", return_value=make_response(404)):
            with pytest.raises(FakestoreHTTPError, match="404"):
                extractor.get_products()

    def test_status_inattendu_leve_http_error(self, extractor):
        with patch.object(extractor.session, "get", return_value=make_response(403)):
            with pytest.raises(FakestoreHTTPError, match="403"):
                extractor.get_products()

    def test_500_retente_et_leve_max_retries(self, extractor):
        extractor.MAX_RETRIES = 3
        with patch.object(extractor.session, "get", return_value=make_response(500)):
            with patch("time.sleep"):
                with pytest.raises(FakestoreMaxRetriesError):
                    extractor.get_products()


# ─── Tests erreurs réseau ─────────────────────────────────────────────────────

class TestNetworkErrors:

    def test_timeout_retente_et_leve_max_retries(self, extractor):
        extractor.MAX_RETRIES = 3
        with patch.object(extractor.session, "get", side_effect=Timeout()):
            with patch("time.sleep"):
                with pytest.raises(FakestoreMaxRetriesError):
                    extractor.get_products()

    def test_connection_error_retente_et_leve_max_retries(self, extractor):
        extractor.MAX_RETRIES = 3
        with patch.object(extractor.session, "get", side_effect=ConnectionError()):
            with patch("time.sleep"):
                with pytest.raises(FakestoreMaxRetriesError):
                    extractor.get_products()

    def test_retry_reussit_au_2eme_essai(self, extractor):
        """Simule un échec réseau puis un succès."""
        fake_data = [{"id": 1}]
        responses = [
            ConnectionError(),
            make_response(200, fake_data),
        ]
        with patch.object(extractor.session, "get", side_effect=responses):
            with patch("time.sleep"):
                result = extractor.get_products()
        assert result == fake_data


# ─── Tests validation contenu ─────────────────────────────────────────────────

class TestContentValidation:

    def test_json_invalide_leve_invalid_json_error(self, extractor):
        with patch.object(extractor.session, "get", return_value=make_response(200, raise_json=True)):
            with pytest.raises(FakestoreInvalidJSONError):
                extractor.get_products()

    def test_reponse_vide_leve_empty_response_error(self, extractor):
        with patch.object(extractor.session, "get", return_value=make_response(200, [])):
            with pytest.raises(FakestoreEmptyResponseError):
                extractor.get_products()