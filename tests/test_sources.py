from unittest.mock import MagicMock
import pytest
import requests
from etl import sources


@pytest.mark.parametrize('status,expected', [(404, sources.SourceNotReadyError), (503, ConnectionError)])
def test_missing_release_is_distinct_from_operational_failure(monkeypatch, status, expected):
    response = requests.Response()
    response.status_code = status
    cause = requests.HTTPError(response=response)
    def fail(*args):
        try:
            raise cause
        except requests.HTTPError as exc:
            raise ConnectionError('Source download failed') from exc
    monkeypatch.setattr(sources, '_load', fail)
    with pytest.raises(expected):
        sources.load_season_tables(2026)
