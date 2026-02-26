import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DBNAME')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
sslmode = os.getenv('SSLMODE')

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        sslrootcert=os.path.expanduser('~/.postgresql/root.crt')
    )
    
    q = conn.cursor()
    q.execute('SELECT version()')
    version = q.fetchone()
    print(f"Подключение успешно!")
    
    q.close()
    conn.close()
    
except Exception as e:
    print(f"Ошибка подключения: {e}")