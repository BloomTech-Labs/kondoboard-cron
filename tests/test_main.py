from starlette.testclient import TestClient
from src.app.main import app

client = TestClient(app)

def test_home():
    response = client.get("/")
    assert response.status_code == 200


def test_start():
    response = client.get("/start")
    assert response.status_code == 200
