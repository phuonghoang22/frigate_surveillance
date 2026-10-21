FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt psycopg2-binary
COPY main.py .
CMD ["python", "-u", "main.py"]
