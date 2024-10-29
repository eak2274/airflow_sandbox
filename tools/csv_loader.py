import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Date

engine = create_engine(
    "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}".format(
        dialect="postgresql",
        driver="psycopg2",
        username="airflow",
        password="airflow",
        host="localhost",
        port=5432,
        database="test"
    )
)

# Загружаем данные из CSV в DataFrame
csv_file_path = "../data/Orders.csv"
df = pd.read_csv(csv_file_path)

# Задаем тип данных для столбца с датой
dtype_mapping = {
    'date': Date  # замените 'date_column' на название столбца в вашем DataFrame
}


with engine.begin() as connection:
    df.to_sql(
        name='orders',
        con=connection,
        if_exists='replace',
        index=False,
        dtype=dtype_mapping,
    )
