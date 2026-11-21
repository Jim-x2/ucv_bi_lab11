
import requests
import boto3
import json
from datetime import datetime, timezone

# Configuración
API_URL = "https://dummyjson.com/products?limit=0"
BUCKET_NAME = "ucv-data-platform"

S3_ENDPOINT = "http://floci:4566"

# Consumir API
print("Consultando API DummyJSON...")

response = requests.get(API_URL, timeout=30)
response.raise_for_status()

data = response.json()
products = data["products"]

print(f"Productos obtenidos: {len(products)}")

# Crear cliente S3
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

# Crear archivo JSON
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

file_name = f"products_{timestamp}.json"

json_data = json.dumps(data, ensure_ascii=False, indent=2)

# Subir a S3 Bronze
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=f"products/bronze/{file_name}",
    Body=json_data.encode("utf-8"),
    ContentType="application/json"
)

print("Ingesta completada correctamente.")
print(f"Archivo: products/bronze/{file_name}")
