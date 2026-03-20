import json

from requests import Response as RequestsResponse
from requests.structures import CaseInsensitiveDict

from backend.app.routes.airflow_proxy import _build_airflow_response


def _make_response(status_code: int, body: bytes, content_type=None) -> RequestsResponse:
    response = RequestsResponse()
    response.status_code = status_code
    response._content = body
    headers = {}
    if content_type:
        headers['Content-Type'] = content_type
    response.headers = CaseInsensitiveDict(headers)
    return response


def test_build_airflow_response_returns_json_payload():
    response = _make_response(200, b'{"dag_runs": []}', 'application/json')

    fastapi_response = _build_airflow_response(response, context="unit-test-json")

    assert fastapi_response.status_code == 200
    assert json.loads(fastapi_response.body) == {"dag_runs": []}
    assert fastapi_response.media_type == 'application/json'


def test_build_airflow_response_handles_text_payload(caplog):
    response = _make_response(400, b'Bad Request: invalid order_by', 'text/html')

    with caplog.at_level('WARNING'):
        fastapi_response = _build_airflow_response(response, context="unit-test-text")

    assert fastapi_response.status_code == 400
    assert fastapi_response.body == b'Bad Request: invalid order_by'
    assert fastapi_response.media_type == 'text/html'
    assert 'non-JSON payload' in caplog.text
