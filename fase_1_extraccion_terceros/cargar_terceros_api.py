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
                df_empresa['empresa_ter'] = nombre_empresa
                lista_dfs_empresas.append(df_empresa)
                print(f"¡ÉXITO! Se procesaron {len(df_empresa)} clientes de {nombre_empresa}.")
        except Exception as e:
            print(f"ERROR al procesar {nombre_empresa}: {e}")
            
    if lista_dfs_empresas:
        df_consolidado = pd.concat(lista_dfs_empresas, ignore_index=True)
        print(f"\nINFO: Extracción completada. Total de clientes consolidados: {len(df_consolidado)}")
        return df_consolidado
    return None

def cargar_terceros_db(df_terceros, conn):
    """
    Carga el DataFrame de terceros en la tabla 'terceros' usando la estrategia UPSERT 
    (INSERT ON CONFLICT DO UPDATE).
    
    La clave única (ON CONFLICT) es: (nit, empresa_ter)
    """
    table_name = "terceros"
    print(f"\nINFO: Iniciando carga UPSERT en '{table_name}'...")
    
    if df_terceros is None or df_terceros.empty:
        print("Advertencia: No hay datos de terceros para cargar.")
        return

    try:
        # --- Paso 1: Preparación de datos ---
        from psycopg2 import extras

        # Convertimos nulos (NaN) a None (NULL de Python/SQL)
        df_para_insertar = df_terceros.astype(object).where(pd.notnull(df_terceros), None)
        datos_para_insertar = [tuple(row) for row in df_para_insertar.values.tolist()]
        
        columnas_db = list(df_terceros.columns)
        
        # Clave de Conflicto: NIT y EMPRESA
        clave_conflicto = ("nit", "empresa_ter")
        
        # Columnas a actualizar: todas menos la clave de conflicto
        columnas_a_actualizar = [
            col for col in columnas_db if col not in clave_conflicto
        ]
        
        # 2. Construcción de la cláusula ON CONFLICT DO UPDATE
        update_clausule = ', '.join([
            f'"{col}" = EXCLUDED."{col}"' for col in columnas_a_actualizar
        ])
        
        # 3. Consulta SQL final
        # Usamos f-string para el nombre de la tabla y join para las columnas.
        query_upsert = f"""
            INSERT INTO public."{table_name}" ({', '.join(f'"{c}"' for c in columnas_db)})
            VALUES %s
            ON CONFLICT ("nit", "empresa_ter") DO UPDATE
            SET {update_clausule};
        """
        
        # --- Paso 2: Ejecución ---
        with conn.cursor() as cursor:
            print(f"Info: Ejecutando UPSERT para {len(datos_para_insertar)} registros...")
            extras.execute_values(
                cursor, 
                query_upsert, 
                datos_para_insertar, 
                page_size=5000
            )
            conn.commit()
            print(f"¡ÉXITO! {len(datos_para_insertar)} registros de terceros actualizados/insertados en '{table_name}'.")

    except Exception as e:
        print(f"ERROR CRÍTICO durante la carga UPSERT en '{table_name}': {e}")
        conn.rollback()
        raise


def ejecutar_fase_1_terceros():
    """
    Orquesta la extracción, transformación y carga (UPSERT) de los datos de Terceros.
    """
    # Importamos get_db_connection aquí (si no está importado globalmente)
    from utils.db_utils import get_db_connection
    
    print("\n=== INICIO FASE 1: ACTUALIZACIÓN DE TERCEROS ===")
    
    # 1. Extracción y Transformación
    try:
        # Debes reemplazar 'extraer_y_transformar_terceros' con el nombre de tu función de extracción
        df_terceros = extraer_clientes_api() 
    except Exception as e:
        print(f"ERROR: Falló la extracción y transformación de Terceros: {e}")
        return # Salir si la extracción falla

    if df_terceros is not None and not df_terceros.empty:
        # 2. Conexión y Carga (Upsert)
        conn = get_db_connection()
        if conn:
            try:
                # Debes reemplazar 'cargar_terceros_db' con el nombre de tu función de carga
                cargar_terceros_db(df_terceros, conn) 
            except Exception as e:
                print(f"ERROR: El proceso de carga UPSERT de Terceros falló: {e}")
            finally:
                conn.close()
                print("\nConexión a la base de datos cerrada.")
    else:
        print("Advertencia: No se encontraron datos de Terceros para actualizar.")
    
    print("\n== FIN FASE 1: Actualización de Terceros ==\n")