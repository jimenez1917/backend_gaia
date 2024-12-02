from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # AWS Credentials
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_DEFAULT_REGION: str

    # RDS Configuration
    PROD_POSTGRES_DB: str
    PROD_POSTGRES_USER: str
    PROD_POSTGRES_HOST: str
    PROD_POSTGRES_PORT: int

    # Athena Configuration
    ATHENA_DATABASE: str
    ATHENA_OUTPUT_BUCKET: str
    ATHENA_OUTPUT_PREFIX: str

    class Config:
        env_file = "../.env"

settings = Settings()