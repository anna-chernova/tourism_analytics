import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import urllib.parse
import time

# Загружаем переменные окружения из файла .env
load_dotenv()

def connection_db():
    """Создает подключение к базе данных"""
    
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    dbname = os.getenv('DBNAME')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    sslmode = os.getenv('SSLMODE')
    
    pass_shielding = urllib.parse.quote_plus(password)
    conn = f"postgresql://{user}:{pass_shielding}@{host}:{port}/{dbname}?sslmode={sslmode}&sslrootcert={os.path.expanduser('~/.postgresql/root.crt')}"
    engine = create_engine(conn)
    return engine

def data_info(df):
    """Краткая информация о датасете"""
    print(f"Размерность: {df.shape[0]} строк, {df.shape[1]} столбцов")

    print(f"\nПропущенные значения (всего): {df.isna().sum().sum()}")
    if df.isna().sum().sum() > 0:
        print("\nПропущенные значения по столбцам:")
        print(df.isna().sum()[df.isna().sum() > 0])
    
    print(f"\nДубликаты строк: {df.duplicated().sum()}")
    
    print(f"\nТипы данных:")
    print(df.dtypes.value_counts())

def preprocess_data(df):
    """Предобработка данных"""
    
    df_clean = df.copy()
    df_clean.columns = df_clean.columns.str.lower()

    # Заполнение пропусков в строковых колонках
    df_clean['territory_code'] = df_clean['territory_code'].fillna(df_clean['territory_code'].dropna().mode()[0])
    categorical = ['territory_name', 'trip_type', 'visit_type', 'home_country', 'home_region', 'home_city', 'goal', 'gender', 'age', 'income']
    for c in categorical:
        df_clean[c] = df_clean[c].fillna('неизвестно')
    
    # Приведение типов

    df_clean['territory_code'] = df_clean['territory_code'].astype(str)

    df_clean['date_of_arrival'] = pd.to_datetime(df_clean['date_of_arrival'], errors='coerce')
    for c in categorical:
        df_clean[c] = df_clean[c].astype('category')

    df_clean['days_cnt'] = pd.to_numeric(df_clean['days_cnt'], errors='coerce').fillna(0)
    df_clean['visitors_cnt'] = pd.to_numeric(df_clean['visitors_cnt'], errors='coerce').fillna(0)
    df_clean['spent'] = pd.to_numeric(df_clean['spent'], errors='coerce').fillna(0)
    
    df_clean = df_clean.dropna(subset=['date_of_arrival'])
    
    return df_clean

def load_data_to_db(df, table_name='visits'):
    """Загрузка а базу данных"""

    conn = connection_db()
    try:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists='append',
            index=False,
            chunksize=10000
        )
        print(f"Загружено {len(df)} строк")

    except Exception as e:
        print(f"Ошибка загрузки {e}")
        raise

    return len(df)
    
def test_upload_data(table_name='visits'):
    """Проверка загруженных данных"""
    conn = connection_db()

    try:
        with conn.connect() as connection:
            res_rows = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            # print(f"Всего строк: {res_rows.scalar()}")

            res_col = connection.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            # print(f"Столбцов: {res_col.scalar()}")

            print(f"\nТаблица {table_name}: {res_rows.scalar()} строк, {res_col.scalar()} столбцов")
    except Exception as e:
        print(f"Ошибка проверки {e}")
        raise

if __name__ == "__main__":
    
    file_path = "data/final.csv"
    count = 0
    
    for chunk in pd.read_csv(file_path, chunksize=10000):
        chunk_clean = preprocess_data(chunk)
        count += load_data_to_db(chunk_clean)
        print(f"Загружено: {count} строк")
    
    print(f"\nВсего: {count} строк")
    test_upload_data()
    