import pandas as pd
from sqlalchemy import create_engine

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

with engine.begin() as connection:
    df.to_sql(
        name='orders',
        con=connection,
        if_exists='replace',
        index=False,
    )

