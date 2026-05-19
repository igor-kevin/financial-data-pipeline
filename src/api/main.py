from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('.') / '.env')
print(os.getenv('DB_PASSWORD'))


app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
