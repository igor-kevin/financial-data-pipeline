import psycopg
from .config import settings

def get_conn():
    print(f'conning xd: {settings.db_host}, {settings.db_password}')
    con = psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password
    )
    print('conned ja era')
    return con
