from faker import Faker
import pandas as pd
import numpy as np
import random
import os

# =======================================================
# DESAFÍO #1 — Generación Automática de Datos Sintéticos
# =======================================================

# Inicializar Faker en español
fake = Faker("es_ES")
random.seed(42)
np.random.seed(42)

# 1. Departamentos
departamentos = ["Finanzas", "Marketing", "Operaciones", "Recursos Humanos", "IT"]

df_departamentos = pd.DataFrame({
    "department_id": range(1, len(departamentos) + 1),
    "department_name": departamentos,
    "location": [fake.city() for _ in departamentos]
})

# 2. Puestos de Trabajo
# upload_to_blob.py
import os
from azure.storage.blob import BlobServiceClient
from pathlib import Path

# =========================
# Configuración
# =========================
# Guarda tu Connection String en la variable de entorno AZURE_STORAGE_CONNECTION_STRING
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = os.getenv("AZURE_CONTAINER", "synthetic-data")  # contenedor que creaste

DATA_DIR = Path("data")  # carpeta local donde están tus CSV/Parquet

# =========================
# Conexión a Azure Blob
# =========================
blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)

# Crear contenedor si no existe
try:
    blob_service_client.create_container(CONTAINER)
    print(f"✅ Contenedor '{CONTAINER}' creado")
except Exception as e:
    print(f"⚠️ Contenedor '{CONTAINER}' ya existe o error: {e}")

# =========================
# Subir archivos
# =========================
for fname in os.listdir(DATA_DIR):
    file_path = DATA_DIR / fname
    blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=fname)
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"✅ Subido {fname} a Azure Blob Storage")


# 3. Empleados
dominios = ["gmail.com", "outlook.com", "yahoo.es", "empresa.com", "correo.com"]

empleados = []
for i in range(1, 1001): 
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(dominios)}"
    job_id = random.randint(1, len(puestos))
    min_sal = df_puestos.loc[df_puestos["job_id"] == job_id, "min_salary"].values[0]
    max_sal = df_puestos.loc[df_puestos["job_id"] == job_id, "max_salary"].values[0]
    salary = round(random.uniform(min_sal, max_sal))

    empleados.append({
        "employee_id": i,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone_number": fake.phone_number(),
        "department_id": random.randint(1, len(departamentos)),
        "job_id": job_id,
        "salary": salary,
        "hire_date": fake.date_between(start_date="-5y", end_date="today")
    })

df_empleados = pd.DataFrame(empleados)

# =======================================================
# DESAFÍO #2 — Almacenamiento de datos
# =======================================================

os.makedirs("data", exist_ok=True)

# Exportar a CSV
df_departamentos.to_csv("data/departments.csv", index=False, encoding="utf-8")
df_puestos.to_csv("data/jobs.csv", index=False, encoding="utf-8")
df_empleados.to_csv("data/employees.csv", index=False, encoding="utf-8")

# Exportar a Parquet
df_departamentos.to_parquet("data/departments.parquet", index=False)
df_puestos.to_parquet("data/jobs.parquet", index=False)
df_empleados.to_parquet("data/employees.parquet", index=False)
