import pandas as pd
import requests
import os
import sys
from datetime import datetime
from psycopg2 import extras

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from utils.db_utils import get_db_connection

def extraer_clientes_api():
    print("Info: Iniciando extracción de clientes desde la API de TNS...")
    lista_dfs_empresas = []
    mapeo_columnas_api = { 'nit': 'nit_ter', 'codigo': 'cod_cliente_ter', 'nombre': 'nombre_ter', 'codigoClasificacion1': 'cod_clasificacion_ter', 'nombreClasificacion1': 'clasificacion_ter', 'codigoCiudad': 'cod_ciudad_ter', 'nombreCiudad': 'ciudad_ter', 'telefono': 'telefono_ter', 'direccion': 'direccion_ter', 'inactivo': 'inactivo'}
    # Hacemos un bucle para procesar cada empresa
    for empresa_config in config.api_config_tns:
        nombre_empresa = empresa_config["nombre_corto"]
        # Obtenemos las URLs de Login y Terceros desde el config
        url_login = config.api_url["login"]
        url_terceros = config.api_url["tercero"]
        print(f"--- Extrayendo para la empresa: {nombre_empresa} ---")
        
        try:
            print("1. Solicitando token de acceso...")
            login_payload = {
                "codigoEmpresa": empresa_config["empresa_tns"],
                "nombreUsuario": empresa_config["usuario_tns"],
                "contrasenia": empresa_config["password_tns"]
            }
            login_response = requests.post(url_login, json=login_payload, timeout=60)
            login_response.raise_for_status() # Lanza error si el login falla
            token = login_response.json()["data"]
            print("   -> Token obtenido con éxito.")
            
            print("2. Solicitando datos de terceros...")
            # Preparamos las cabeceras (headers) con el token Bearer
            headers = {
                "Authorization": f"Bearer {token}"
            }
            # Preparamos los parámetros de la URL (params)
            #Hacemos la llamada a la API
            response = requests.get(url_terceros, headers=headers, timeout=300)
            response.raise_for_status()
            datos_api_raw = response.json()
            
            #Creamos la lista vacía para guardar los datos de cada empresa
            lista_terceros_api = []
            #Ajustamos al formato de respuesta (dict['data'])
            if isinstance(datos_api_raw, dict) and "data" in datos_api_raw:
                lista_terceros_api = datos_api_raw.get("data")
            if not lista_terceros_api:
                print(f"INFO: No se encontraron registros de terceros para {nombre_empresa}.")
                continue #Saltamos a la siguiente empresa
            terceros_procesados = [ {nuestra_col: item.get(api_col) for api_col, nuestra_col in mapeo_columnas_api.items()} for item in lista_terceros_api if isinstance(item, dict) ]
            if terceros_procesados:
                df_empresa = pd.DataFrame(terceros_procesados)
                df_empresa['empresa_erp'] = nombre_empresa
                lista_dfs_empresas.append(df_empresa)
                print(f"¡ÉXITO! Se procesaron {len(df_empresa)} clientes de {nombre_empresa}.")
        except Exception as e:
            print(f"ERROR al procesar {nombre_empresa}: {e}")
            
    if lista_dfs_empresas:
        df_consolidado = pd.concat(lista_dfs_empresas, ignore_index=True)
        print(f"\nINFO: Extracción completada. Total de clientes consolidados: {len(df_consolidado)}")
        return df_consolidado
    return None