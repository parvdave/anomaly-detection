import os
from dataclasses import dataclass, field


@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
    )
    topic: str = field(
        default_factory=lambda: os.environ.get("CRYPTO_TOPIC", "crypto.prices.raw")
    )
    starting_offsets: str = field(
        default_factory=lambda: os.environ.get("KAFKA_STARTING_OFFSETS", "latest")
    )
    group_id: str = "spark-crypto-consumer"


@dataclass
class DBConfig:
    jdbc_url: str = field(
        default_factory=lambda: os.environ.get(
            "JDBC_URL", "jdbc:postgresql://timescaledb:5432/cryptodb"
        )
    )
    user: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_USER", "cryptouser")
    )
    password: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_PASSWORD", "cryptopassword")
    )
    driver: str = "org.postgresql.Driver"

    @property
    def connection_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }


@dataclass
class AnomalyConfig:
    zscore_threshold: float = field(
        default_factory=lambda: float(os.environ.get("SPARK_ZSCORE_THRESHOLD", "3.0"))
    )
    iqr_multiplier: float = field(
        default_factory=lambda: float(os.environ.get("SPARK_IQR_MULTIPLIER", "1.5"))
    )
    volume_spike_multiplier: float = field(
        default_factory=lambda: float(os.environ.get("SPARK_VOLUME_SPIKE_MULTIPLIER", "3.0"))
    )
    window_size: int = field(
        default_factory=lambda: int(os.environ.get("SPARK_WINDOW_SIZE", "20"))
    )
    min_window_size: int = 5  # minimum rows before detection fires


@dataclass
class SparkConfig:
    app_name: str = "CryptoAnomalyDetection"
    master: str = field(
        default_factory=lambda: os.environ.get("SPARK_MASTER", "local[*]")
    )
    shuffle_partitions: int = 4
    checkpoint_dir: str = field(
        default_factory=lambda: os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    )
    trigger_interval: str = field(
        default_factory=lambda: os.environ.get("SPARK_TRIGGER_INTERVAL", "5 seconds")
    )
    jars_path: str = "/app/jars"
