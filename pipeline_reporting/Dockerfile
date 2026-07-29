FROM python:3.11-slim

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 4003

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4003", "-f", "main.py"]
