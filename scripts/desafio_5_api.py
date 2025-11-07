from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

DB_HOST = os.getenv("SUPABASE_HOST")
DB_NAME = os.getenv("SUPABASE_DB")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_PORT = os.getenv("SUPABASE_PORT", 5432)

app = FastAPI(
    title="MVM Prueba Técnica - API REST",
    description="API para consultar la vista vw_employees_summary desde Supabase",
    version="1.0.0"
)


def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión: {e}")


@app.get("/")
def root():
    return {"message": "Bienvenido a la API de Empleados MVM 🚀"}

# --- Endpoint 1: Obtener todos los empleados ---
@app.get("/employees")
def get_all_employees(limit: int = 50):
    """
    Retorna los primeros empleados desde la vista vw_employees_summary.
    """
    try:
        conn = get_connection()
        query = f"SELECT * FROM vw_employees_summary LIMIT {limit}"
        df = pd.read_sql(query, conn)
        conn.close()
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/employees/department/{department_name}")
def get_by_department(department_name: str):
    """
    Retorna los empleados pertenecientes a un departamento específico.
    """
    try:
        conn = get_connection()
        query = """
        SELECT * FROM vw_employees_summary
        WHERE LOWER(department_name) = LOWER(%s)
        ORDER BY salary DESC
        """
        df = pd.read_sql(query, conn, params=[department_name])
        conn.close()
        if df.empty:
            raise HTTPException(status_code=404, detail="Departamento no encontrado")
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/employees/job/{job_title}")
def get_by_job(job_title: str):
    """
    Retorna los empleados según su cargo (job_title).
    """
    try:
        conn = get_connection()
        query = """
        SELECT * FROM vw_employees_summary
        WHERE LOWER(job_title) = LOWER(%s)
        ORDER BY salary DESC
        """
        df = pd.read_sql(query, conn, params=[job_title])
        conn.close()
        if df.empty:
            raise HTTPException(status_code=404, detail="Cargo no encontrado")
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
