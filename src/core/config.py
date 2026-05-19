from pydantic_settings import BaseSettings 
from pathlib import Path

class Settings(BaseSettings):
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    
    class Config:
        env_file = str(Path('.') / '.env')

   
settings = Settings()