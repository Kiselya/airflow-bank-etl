# Взяли   образ
FROM apache/airflow:3.0.2

# Копируем наш файл с зависимостями внутрь образа
COPY requirements.txt .

# Устанавливаем зависимости из файла
RUN pip install --no-cache-dir -r requirements.txt