import io

import pytest

from backend.api import chart_images


class DummyDB:
    def __init__(self):
        self.committed = False
        self.refreshed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def refresh(self, _):
        self.refreshed = True

    def rollback(self):
        self.rolled_back = True


class DummyArtifact:
    def __init__(self):
        self.id = 24
        self.symbol = 'TSLA'
        self.chart_type = 'daily'
        self.artifact_metadata = {
            'market_data_snapshot': {
                'symbol': 'TSLA',
                'bars': [
                    {'date': '2025-11-14T00:00:00', 'open': 248, 'high': 252, 'low': 247, 'close': 251, 'volume': 2990000},
                    {'date': '2025-11-15T00:00:00', 'open': 251, 'high': 253, 'low': 249, 'close': 252, 'volume': 3000000}
                ]
            }
        }
        self.chart_data = {}
        self.image_path = None


class DummyMinio:
    def __init__(self):
        self.uploaded = False

    def bucket_exists(self, bucket_name):
        return True

    def make_bucket(self, bucket_name):  # pragma: no cover - not called in test
        pass

    def put_object(self, bucket_name, object_name, data_stream, length, content_type):
        self.uploaded = True
        # consume stream to simulate upload
        data_stream.read()


@pytest.fixture(autouse=True)
def reset_minio(monkeypatch):
    # Patch chart rendering to avoid heavy dependencies
    def fake_generate_chart(df, symbol, width, height):
        buf = io.BytesIO()
        buf.write(b'test-image')
        buf.seek(0)
        return buf

    monkeypatch.setattr(chart_images, 'generate_technical_chart', fake_generate_chart)
    dummy_client = DummyMinio()
    monkeypatch.setattr(chart_images, 'minio_client', dummy_client)
    monkeypatch.setattr(chart_images, 'MINIO_AVAILABLE', True)
    return dummy_client


def test_regenerate_chart_from_metadata_success(reset_minio):
    artifact = DummyArtifact()
    db = DummyDB()

    data = chart_images._regenerate_chart_from_metadata(artifact, db)

    assert data == b'test-image'
    assert artifact.image_path is not None and artifact.image_path.startswith('http://')
    assert artifact.chart_data['minio_url'] == artifact.image_path
    assert db.committed is True
