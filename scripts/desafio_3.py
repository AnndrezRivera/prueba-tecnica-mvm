from azure.storage.blob import BlobServiceClient
from pathlib import Path
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

load_dotenv()

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = os.getenv("AZURE_CONTAINER")
DATA_DIR = Path("data")

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)

try:
    blob_service_client.create_container(CONTAINER)
    print(f"Contenedor '{CONTAINER}' creado")
except Exception as e:
    print(f"Contenedor '{CONTAINER}' ya existe o error: {e}")

for file_path in DATA_DIR.glob("*.parquet"):
    blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=file_path.name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Subido {file_path.name} a Azure Blob Storage")


SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", 5432)

conn = psycopg2.connect(
    host=SUPABASE_HOST,
    dbname=SUPABASE_DB,
    user=SUPABASE_USER,
    password=SUPABASE_PASSWORD,
    port=SUPABASE_PORT
)
cur = conn.cursor()
print("✅ Conectado a Supabase")


df_departamentos = pd.read_parquet("data/departments.parquet")

cur.execute("""
DROP TABLE IF EXISTS departamentos CASCADE;
CREATE TABLE departamentos (
    department_id SERIAL PRIMARY KEY,
    department_name TEXT,
    location TEXT
);
""")

execute_values(
    cur,
    "INSERT INTO departamentos (department_id, department_name, location) VALUES %s",
    df_departamentos.values.tolist()
)
print(f"Tabla 'departamentos' cargada ({len(df_departamentos)} filas)")


df_puestos = pd.read_parquet("data/jobs.parquet")

cur.execute("""
DROP TABLE IF EXISTS puestos CASCADE;
CREATE TABLE puestos (
    job_id SERIAL PRIMARY KEY,
    job_title TEXT,
    min_salary NUMERIC,
    max_salary NUMERIC
);
""")

execute_values(
    cur,
    "INSERT INTO puestos (job_id, job_title, min_salary, max_salary) VALUES %s",
    df_puestos.values.tolist()
)
print(f"Tabla 'puestos' cargada ({len(df_puestos)} filas)")


df_empleados = pd.read_parquet("data/employees.parquet")

cur.execute("""
DROP TABLE IF EXISTS empleados CASCADE;
CREATE TABLE empleados (
    employee_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone_number TEXT,
    department_id INT REFERENCES departamentos(department_id),
    job_id INT REFERENCES puestos(job_id),
    salary NUMERIC,
    hire_date DATE
);
""")

execute_values(
    cur,
    """
    INSERT INTO empleados (
        employee_id, first_name, last_name, email, phone_number,
        department_id, job_id, salary, hire_date
    ) VALUES %s
    """,
    df_empleados.values.tolist()
)
print(f"Tabla 'empleados' cargada ({len(df_empleados)} filas)")

conn.commit()
cur.close()
conn.close()
print("Carga finalizada correctamente con relaciones entre tablas")