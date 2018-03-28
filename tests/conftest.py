import pytest
import apistar


@pytest.fixture
@pytest.mark.asyncio
def client():
    apistar.TestClient(app)
