from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://cryptouser:cryptopassword@timescaledb:5432/cryptodb"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    ws_heartbeat_interval: int = 30
    anomaly_page_size: int = 100

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
