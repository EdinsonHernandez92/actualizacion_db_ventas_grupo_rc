# Funciones de utilidad para interactuar con la base de datos
 
import psycopg2
from psycopg2 import extras
import config

def get_db_connection():
    """Establece y retorna una conexión a la base de datos PostgreSQL"""
    try:
        if not config.db_config.get("password"):
            raise ValueError("La contraseña de la BD (db_password) no está en el archivo .env")
        
        conn = psycopg2.connect(**config.db_config)
        return conn
    except (psycopg2.Error, ValueError) as e:
        print(f"Error Crítico: No se pudo conectar a la base de datos {e}")
        return None

def execute_query(conn, query, params=None, fetch=None):
    """
    Ejecuta una consulta SQL.
    :param fetch: 'one' para un resultado, 'all' para todos, None para no obtener resultados.
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, params)
            if fetch == 'one':
                return cursor.fetchone()
            elif fetch == 'all':
                return cursor.fetchall()
            else:
                conn.commit()
        except psycopg2.Error as e:
            print(f"Error al ejecutar query: {e}")
            conn.rollback()
            raise

def delete_by_date_range(conn, table_name, fecha_sql_inicial, fecha_sql_fin, empresa_nombre):
    """
    Elimina filas de una tabla según un rango de fechas y una empresa usando DELETE.
    """
    print(f"Info: Eliminando datos de '{table_name}' entre {fecha_sql_inicial} y {fecha_sql_fin} para '{empresa_nombre}'...")

    query = f"""
        DELETE FROM public."{table_name}"
        WHERE fecha BETWEEN %s AND %s
        AND empresa = %s;
    """

    params = (fecha_sql_inicial, fecha_sql_fin, empresa_nombre)

    try:
        execute_query(conn, query, params) 
        
        print(f"¡ÉXITO! Los datos del rango han sido eliminados de '{table_name}'.")
    except psycopg2.Error as e:
        print(f"ERROR: No se pudo eliminar de '{table_name}'. Error: {e}")
        raise
    except Exception as e:
        print(f"Error de código: ¿Tu función 'execute_query' acepta un segundo argumento para los parámetros? {e}")
        raise

def copy_csv_to_db(conn, csv_filepath, table_name):
    """
    Carga datos desde un archivo CSV a una tabla de PostgreSQL usando COPY FROM.
    Es el método más rápido para cargas masivas.
    """
    print(f"Info: Iniciando carga masiva (COPY) a las tabla '{table_name}' desde '{csv_filepath}'...")
    with conn.cursor() as cursor:
        try:
            with open(csv_filepath, 'r', encoding='utf-8') as f:
                # El csv debe tener cabecera y coincidir con las columnas de la tabla
                cursor.copy_expert(f"COPY public.\"{table_name}\" FROM STDIN WITH CSV HEADER", f)
            conn.commit()
            print(f"¡Éxito! Datos cargados a la tabla '{table_name}'.")
        except FileNotFoundError:
            print(f"Error: Archivo csv no encontrado en {csv_filepath}")
            raise
        except psycopg2.Error as e:
            print(f"Error al ejecutar copy en la tabla '{table_name}': {e}")
            conn.rollback()
            raise