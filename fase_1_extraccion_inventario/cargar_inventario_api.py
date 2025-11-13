import pandas as pd
import requests
import os
import sys
from datetime import datetime
from psycopg2 import extras

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from utils.db_utils import get_db_connection

def extraer_y_transformar_inventario():
    """
    Extrae y transforma los datos de inventario desde la API, aplicando la lógica de negocio.
    """
    print("Info: Iniciando extracción y transformación de inventario...")

    lista_dfs_finales = []
    
    for empresa_config in config.api_config_tns:
        nombre_empresa = empresa_config["nombre_corto"]

        #Llamamos desde el config las URLs de login y productos
        url_login = config.api_url["login"]
        url_productos = config.api_url["productos"]

        print(f"Procesando inventario para: {nombre_empresa}")

        try:
            #Flujo de autenticación
            # Paso 1. Obtenemos el token de acceso
            print("1. Solicitando token de acceso...")
            login_payload = {
                "codigoEmpresa": empresa_config["empresa_tns"],
                "nombreUsuario": empresa_config["usuario_tns"],
                "contrasenia": empresa_config["password_tns"]                
            }
            login_response = requests.post(url_login, json=login_payload, timeout=60)
            login_response.raise_for_status() #Lanza error si el login falla
            token = login_response.json()["data"]
            print("Token obtenido con éxito.")
            # Paso 2. Consultar productos con el token
            print("2. Solicitando datos de inventario.")
            #Preparamos los encabezados (headers) con el token Bearer
            headers = {
                "Authorization": f"Bearer {token}"
            }
            #Preparamos los parámetros de la URL (params)
            params_productos = {
                "codigosucursal": "00"
            }
            #Hacemos la llamada a la API
            response = requests.get(url_productos, headers=headers, params=params_productos, timeout=300)
            response.raise_for_status()
            datos_api_raw = response.json()
            if not (isinstance(datos_api_raw, dict) and "data" in datos_api_raw): continue

            #Aplanamos el json directamente
            df_empresa = pd.json_normalize(
                datos_api_raw.get("data"),
                record_path='bodegas',
                meta=['codigo', 'referencia', 'listaPrecios'],
                errors='ignore' #Ignora productos sin la estructura de bodegas
            )
            if df_empresa.empty:continue

            #Añadimos la columna de la empresa
            df_empresa['empresa_inv'] = nombre_empresa
            
            #Aplicamos los filtros de negocio
            bodegas_permitidas = empresa_config.get("bodegas_permitidas",[])
            df_empresa= df_empresa[df_empresa['codigoBodega'].isin(bodegas_permitidas)]

            if nombre_empresa in ["CAMDUN", "GMD", "PY"]:
                lista_precio_permitida = empresa_config.get("lista_precio_permitida", "1")
                mask = df_empresa['listaPrecios'].apply(lambda listaPrecios: isinstance(listaPrecios, list) and any(str(listaPrecio.get("codigo", "")).strip() == lista_precio_permitida for listaPrecio in listaPrecios))
                df_empresa = df_empresa[mask]
            if not df_empresa.empty:
                lista_dfs_finales.append(df_empresa)
                print(f"¡Éxito! Se procesaron {len(df_empresa)} registros de inventario para {nombre_empresa}.")
        except Exception as e:
            print(f"Error al procesar {nombre_empresa}: {e}")
    
    if lista_dfs_finales:
        df_consolidado = pd.concat(lista_dfs_finales, ignore_index=True)
        df_consolidado = df_consolidado.rename(columns={
            'codigo': 'codigo_inv', 'referencia': 'referencia_inv', 'descripcion':'descripcion_inv',
            'codigoBodega':'bodega_inv', 'existencias': 'existencias_inv'
        })
        columnas_finales = ['codigo_inv', 'referencia_inv', 'descripcion_inv', 'bodega_inv', 'existencias_inv', 'empresa_inv']
        df_consolidado = df_consolidado[columnas_finales]
        print(f"\nInfo: Extracción completada. Total de registros de inventario: {len(df_consolidado)}")
        return df_consolidado
    return None

def cargar_inventario_db(df_inventario, conn):
    """
    Carga el DataFrame de inventario en la tabla 'inventario' usando la estrategia UPSERT 
    (INSERT ON CONFLICT DO UPDATE).
    
    La clave única (ON CONFLICT) es: (codigo_inv, bodega_inv, empresa_inv)
    """
    table_name = "inventario"
    print(f"\nINFO: Iniciando carga UPSERT en '{table_name}'...")
    
    if df_inventario is None or df_inventario.empty:
        print("Advertencia: No hay datos de inventario para cargar.")
        return

    try:
        # --- Paso 1: Preparación de datos ---
        # 1. Convertimos todos los nulos (NaN) a None (NULL de Python/SQL)
        df_para_insertar = df_inventario.astype(object).where(pd.notnull(df_inventario), None)

        # 2. Convertimos el DataFrame a una lista de tuplas para execute_values
        datos_para_insertar = [tuple(row) for row in df_para_insertar.values.tolist()]
        
        # Obtenemos la lista de columnas (usada en la cláusula INSERT)
        columnas_db = list(df_inventario.columns)
        
        # Creamos la lista de columnas para la cláusula UPDATE
        # Excluimos las columnas que son parte de la clave de conflicto.
        columnas_a_actualizar = [
            col for col in columnas_db if col not in ('codigo_inv', 'bodega_inv', 'empresa_inv')
        ]
        
        # 3. Construcción de la consulta UPSERT
        
        # Cláusula ON CONFLICT DO UPDATE: columna = EXCLUDED.columna
        update_clausule = ', '.join([
            f'"{col}" = EXCLUDED."{col}"' for col in columnas_a_actualizar
        ])
        
        # 4. Consulta SQL final
        query_upsert = f"""
            INSERT INTO public."{table_name}" ({', '.join(f'"{c}"' for c in columnas_db)})
            VALUES %s
            ON CONFLICT ("codigo_inv", "bodega_inv", "empresa_inv") DO UPDATE
            SET {update_clausule};
        """
        
        # --- Paso 2: Ejecución ---
        with conn.cursor() as cursor:
            print(f"Info: Ejecutando UPSERT para {len(datos_para_insertar)} registros...")
            extras.execute_values(
                cursor, 
                query_upsert, 
                datos_para_insertar, 
                page_size=5000 # Un tamaño de página grande para eficiencia
            )
            conn.commit()
            print(f"¡ÉXITO! {len(datos_para_insertar)} registros de inventario actualizados/insertados en '{table_name}'.")

    except Exception as e:
        print(f"ERROR CRÍTICO durante la carga UPSERT en '{table_name}': {e}")
        conn.rollback() # Revertimos si hay un error
        raise # Relanzamos el error para que la orquestación lo maneje

def ejecutar_fase_1_inventario():
    """
    Orquesta la extracción, transformación y carga del inventario.
    """
    print("\n=== INICIO FASE 1: ACTUALIZACIÓN DE INVENTARIO ===")
    
    df_inventario = extraer_y_transformar_inventario()
    
    if df_inventario is not None and not df_inventario.empty:
        conn = get_db_connection()
        if conn:
            try:
                cargar_inventario_db(df_inventario, conn)
            except Exception as e:
                print(f"ERROR: El proceso de carga a la base de datos falló: {e}")
            finally:
                conn.close()
                print("\nConexión a la base de datos cerrada.")
    
    print("\n== FIN FASE 1: Actualización de Inventario ==\n")