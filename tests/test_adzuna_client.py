import pytest
import requests

from ingestion.adzuna_client import AdzunaClient, AdzunaError

SECRET = "super-secret-key"


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response=None, exc=None):
        self.response, self.exc, self.last_params = response, exc, None

    def get(self, url, params, timeout):
        self.last_params = params
        if self.exc:
            raise self.exc
        return self.response


def client(session):
    return AdzunaClient("id", SECRET, min_seconds_between_calls=0, session=session)


def test_missing_keys_fail_fast_with_helpful_message():
    with pytest.raises(AdzunaError, match="--source sample"):
        AdzunaClient("", "")


def test_search_sends_expected_params_and_returns_json():
    session = FakeSession(FakeResponse(200, {"results": [{"id": "1"}]}))
    assert client(session).search("data engineer", 1) == {"results": [{"id": "1"}]}
    assert session.last_params["title_only"] == "data engineer"
    assert session.last_params["results_per_page"] == 50


def test_http_error_message_does_not_leak_key():
    with pytest.raises(AdzunaError) as err:
        client(FakeSession(FakeResponse(401))).search("data engineer", 1)
    assert "401" in str(err.value)
    assert SECRET not in str(err.value)


def test_network_error_message_does_not_leak_key():
    exc = requests.ConnectionError(f"failed for https://api?app_key={SECRET}")
    with pytest.raises(AdzunaError) as err:
        client(FakeSession(exc=exc)).search("data engineer", 1)
    assert SECRET not in str(err.value)
    assert err.value.__cause__ is None  # original exception (with the URL) is dropped
