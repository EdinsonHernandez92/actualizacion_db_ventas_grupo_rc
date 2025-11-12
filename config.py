# Archivo central de configuración del proyecto.

import os
from dotenv import load_dotenv

# --- Carga de Variables de Entorno ---
# Buscar un archivo .env en la raíz del proyecto y carga sus variables.
load_dotenv()
print("INFO: Archivo .env cargado.")

# --- Configuración de la Base de Datos PostgreSQL ---
db_config = {
    "dbname": os.getenv("db_name","ventas_prueba"),
    "user": os.getenv("db_user","postgres"),
    "password": os.getenv("db_password"),
    "host": os.getenv("db_host", "localhost"),
    "port": os.getenv("db_port","5432")
}

# --- Configuración de la API de TNS para cada empresa ---
api_config_tns = [
    {
        "nombre_corto": "CAMDUN",
        "empresa_tns": os.getenv("tns_api_empresa_camdun"),
        "usuario_tns": os.getenv("tns_api_user_camdun"),
        "password_tns": os.getenv("tns_api_password_camdun"),
        "cod_sucursal_tns": "00",
        "bodegas_permitidas": ["00", "06", "09", "10", "11"],
        "lista_precio_permitida": "1"
    },
    {
        "nombre_corto": "GMD",
        "empresa_tns": os.getenv("tns_api_empresa_gmd"),
        "usuario_tns": os.getenv("tns_api_user_gmd"),
        "password_tns": os.getenv("tns_api_password_gmd"),
        "cod_sucursal_tns": "00",
        "bodegas_permitidas": ["00", "06"],
        "lista_precio_permitida": "1"        
    },
    {
        "nombre_corto": "PY",
        "empresa_tns": os.getenv("tns_api_empresa_py"),
        "usuario_tns": os.getenv("tns_api_user_py"),
        "password_tns": os.getenv("tns_api_password_py"),
        "cod_sucursal_tns": "00",
        "bodegas_permitidas": ["03"],
        "lista_precio_permitida": None # No aplica para la fábrica, lo ponemos como None
    }    
]

# --- URLs de la api de TNS ---
api_base_url = os.getenv("tns_api_base_url", "https://api.tns.co/v2")
api_url = {
    "login": f"{api_base_url}/Acceso/Login",
    "productos": f"{api_base_url}/tablas/Material/Listar",
    "ventas": f"{api_base_url}/facturacion/Reportes/ObtenerVentasDetallada",
    "tercero": f"{api_base_url}/tablas/Tercero/Listar"
}

# --- Rutas de direcotrios del proyecto ---
base_dir = os.path.dirname(os.path.abspath(__file__))

# --- CONFIGURACIÓN FASE 3: EXPORTACIÓN ---
# Usamos plantillas (f-strings) para las rutas
# {mes_num} -> "10"
# {mes_nombre} -> "Octubre"
# {anio} -> "2025"

#rutas_exportacion = {
    ## El nombre de la empresa (llave) se usará en la consulta SQL
    #"CAMDUN": r"G:\Mi unidad\0. Ventas diarias\1. Ventas Grupo RC\{mes_num}. Ventas {mes_nombre} CAMDUN.xlsx",
    #"GLOBAL MARCAS": r"G:\Mi unidad\0. Ventas diarias\1. Ventas Grupo RC\{mes_num}. Ventas {mes_nombre} GMD.xlsx",
    #"YERMAN": r"C:\Users\USUARIO\Documents\1. Edinson Hernández\3. Ventas diarias\3. PY\{anio}\2. {mes_nombre}\{mes_num}. Venta {mes_nombre} PY.xlsx"
#}

rutas_exportacion = {
    # El nombre de la empresa (llave) se usará en la consulta SQL
    "CAMDUN": r"G:\Mi unidad\0. Ventas diarias\prueba_export_to_xlsx\{mes_num}. Ventas {mes_nombre} CAMDUN.xlsx",
    "GMD": r"G:\Mi unidad\0. Ventas diarias\prueba_export_to_xlsx\{mes_num}. Ventas {mes_nombre} GMD.xlsx",
    "PY": r"G:\Mi unidad\0. Ventas diarias\prueba_export_to_xlsx\{mes_num}. Ventas {mes_nombre} PY.xlsx",
}