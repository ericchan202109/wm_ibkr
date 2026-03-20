import pytest

from backend.app.routes.airflow_proxy import DagRunFailError, _fail_dag_run


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, text=''):
        self.status_code = status_code
        self._json_data = json_data
        self._text = text
        self.headers = {}
        self.content = text.encode() if isinstance(text, str) else text

    def json(self):
        if self._json_data is None:
            raise ValueError('No JSON payload')
        return self._json_data

    @property
    def text(self):  # type: ignore[override]
        return self._text


class DummySession:
    def __init__(self, run_response, patch_response=None):
        self._run_response = run_response
        self._patch_response = patch_response
        self.last_patch_url = None
        self.last_patch_body = None

    def get(self, url, params=None):
        self.last_get_url = url
        return self._run_response

    def patch(self, url, json=None):
        self.last_patch_url = url
        self.last_patch_body = json
        if self._patch_response is None:
            raise AssertionError('patch should not be called')
        return self._patch_response


def test_fail_dag_run_success_sets_state_to_failed():
    session = DummySession(
        run_response=DummyResponse(200, {'state': 'running'}),
        patch_response=DummyResponse(200, {'state': 'failed'})
    )

    result = _fail_dag_run(session, 'demo_dag', 'scheduled__123')

    assert result['previous_state'] == 'running'
    assert result['new_state'] == 'failed'
    assert session.last_patch_body == {'state': 'failed'}
    assert 'airflow_response' in result


def test_fail_dag_run_conflict_when_already_complete():
    session = DummySession(
        run_response=DummyResponse(200, {'state': 'success'})
    )

    with pytest.raises(DagRunFailError) as exc:
        _fail_dag_run(session, 'demo_dag', 'scheduled__456')

    assert exc.value.status_code == 409
    assert exc.value.payload['current_state'] == 'success'
    assert session.last_patch_url is None


def test_fail_dag_run_surface_airflow_errors():
    session = DummySession(
        run_response=DummyResponse(200, {'state': 'running'}),
        patch_response=DummyResponse(500, None, text='Internal Error')
    )

    with pytest.raises(DagRunFailError) as exc:
        _fail_dag_run(session, 'demo_dag', 'scheduled__789')

    assert exc.value.status_code == 500
    assert 'Internal Error' in exc.value.payload['airflow_response']
