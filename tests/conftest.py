import pytest

from quilr_gateway.config import Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(
        _env_file=None,
        jwt_secret="test-secret-" + "x" * 32,
        database_path=str(tmp_path / "budget.sqlite3"),
        primary_url="http://provider/primary",
        secondary_url="http://provider/secondary",
        mcp_upstream_url="http://provider/mcp",
    )
