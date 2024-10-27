FROM apache/airflow:latest

# Обновление pip и установка Poetry
RUN pip install --upgrade pip && \
    pip install --no-cache-dir poetry

# Отключаем создание виртуального окружения в Poetry
ENV POETRY_VIRTUALENVS_CREATE=false

# Копирование файлов проекта
COPY pyproject.toml poetry.lock ./

# Установка зависимостей через Poetry
RUN poetry install --no-interaction --no-ansi --no-root