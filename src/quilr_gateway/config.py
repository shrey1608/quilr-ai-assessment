from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    jwt_secret: SecretStr = Field(min_length=32)
    jwt_issuer: str = "quilr-assessment"
    jwt_audience: str = "quilr-gateway"
    database_path: str = "data/gateway.sqlite3"
    tokens_per_minute: int = Field(default=50_000, gt=0)
    mcp_upstream_url: str = "http://127.0.0.1:8001/mcp"
    primary_url: str = "http://127.0.0.1:8001/primary/chat/completions"
    secondary_url: str = "http://127.0.0.1:8001/secondary/chat/completions"
    primary_model: str = "mock-primary"
    secondary_model: str = "mock-secondary"
    primary_api_key: SecretStr = SecretStr("")
    secondary_api_key: SecretStr = SecretStr("")
    provider_timeout_seconds: float = Field(default=3.0, gt=0, le=3.0)
