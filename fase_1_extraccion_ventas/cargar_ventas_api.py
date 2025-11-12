import pandas as pd
import numpy as np
import requests
import os
import sys
from datetime import date, timedelta, datetime
from psycopg2 import extras

# --- Configuración del Proyecto ---
# Añadimos la ruta raíz del proyecto al path de python para poder importar nuestro módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config #Importamos nuestras configuraciones (URLs, credenciales)
from utils.db_utils import get_db_connection, execute_query, delete_by_date_range # Importamos nuestras funciones de base de datos

def extraer_ventas_api(fecha_desde, fecha_hasta):
    """
    Paso 1: Extracción
    Nos conectamos a la API de TNS y extraemos los datos de ventas crudos para un rango de fechas
    """
    print(f"Info: Iniciando extracción de ventas desde {fecha_desde} hasta {fecha_hasta}...")
    #Creamos la lista vacía para guardar los datos de cada empresa
    lista_dfs_empresas = []
    mapeo_columnas_api = {
        'nittri': 'nit','nombre':'nombre', 'numfactura':'factura', 'formapago': 'forma_pago', 'fecha':'fecha',
        'codigo': 'codigo', 'descrip':'descripcion', 'codgrupart':'codigo_grupo_art', 'nomgrupart':'nombre_grupo_art', 'unidad':'und',
        'cant':'cant', 'prebase':'valor_base', 'preiva': 'iva', 'porciva':'porc_iva', 'descuento':'descuento',
        'preciotot':'valor', 'listaprecio': 'lista_precio', 'preciolista': 'precio', 'preciolistamayor':'precio_mayor', 'codlinea':'cod_linea',
        'deslinea':'desc_linea', 'codcliente':'cod_cliente','nomclasifica':'clasificacion', 'nomclasifica2':'clasificacion_py','zona':'zona',
        'teleF1':'telefono', 'ciudad':'ciudad', 'observ':'observaciones', 'direcC1':'direccion', 'codparcela':'cod_area',
        'nomaread': 'nom_area', 'codvendedor': 'cod_vendedor', 'nomvendedor':'nom_vendedor', 'nitdes':'cod_despachar', 'despachara':'cliente_despachar',
        'marca':'marca', 'referencia':'referencia', 'barrio':'barrio', 'descripciondep':'dep_articulo', 'recargo':'recargo',
        'fecvenlote': 'serial', 'peso':'peso_bruto', 'factor':'factor','supervisor':'supervisor', 'costopromedio':'costo',
        'motivodevolucion':'motivo_dev', 'pedido':'pedido_tiendapp', 'codbodega': 'bodega'
    }

    # Hacemos un bucle para procesar cada empresa
    for empresa_config in config.api_config_tns:
        nombre_empresa = empresa_config["nombre_corto"]
        #Obtenemos las URLs de acceso y ventas desde el config
        url_login = config.api_url["login"]
        url_ventas = config.api_url["ventas"]

        print(f"--- Iniciando proceso de extracción para: {nombre_empresa} ---")

        try:
            # Implementación flujo de autenticación
            # Paso 1: Obtener el token de acceso
            print("1. Solicitando token de acceso...")
            login_payload = {
                "codigoEmpresa": empresa_config["empresa_tns"],
                "nombreUsuario": empresa_config["usuario_tns"],
                "contrasenia": empresa_config["password_tns"]                
            }
            login_response = requests.post(url_login,json=login_payload,timeout=60)
            login_response.raise_for_status()
            token = login_response.json()["data"]
            print("Token obtenido con éxito.")

            # Paso 2: Consultar ventas con el token
            print("2. Solicitando datos de ventas.")
            # Preparamos las cabeceras (headers) con el token Bearer
            headers = {
                "Authorization": f"Bearer {token}"
            }
            #Preparamos los parámetros de la URL (params)
            #params_ventas = {
            #    "fechaInicial": datetime.strptime(fecha_desde, "%Y-%m-%d").strftime("%m/%d/%Y"),
            #    "fechaFin": datetime.strptime(fecha_hasta, "%Y-%m-%d").strftime("%m/%d/%Y"),
            #    "codigosucursal": "00"
            #}

            # Aseguramos que las fechas enviadas a la API estén en el formato que la API espera (ej. "MM/DD/YYYY")
            fecha_inicial_api_str = datetime.strptime(fecha_desde, "%Y-%m-%d").strftime("%m/%d/%Y")
            fecha_fin_api_str = datetime.strptime(fecha_hasta, "%Y-%m-%d").strftime("%m/%d/%Y")

            params_ventas = {
                "fechaInicial": fecha_inicial_api_str,
                "fechaFin": fecha_fin_api_str,
                "codigosucursal": "00"
            }

            #Hacemos la llamada a la API
            response = requests.get(url_ventas, headers=headers, params=params_ventas, timeout=600)
            response.raise_for_status()
            datos_api_raw = response.json()

            #Ajustamos al formato de respuesta (dict['data'])
            if isinstance(datos_api_raw, dict) and "data" in datos_api_raw:
                lista_ventas = datos_api_raw.get("data")
                
                if not lista_ventas:
                    print(f"Info: No se encontraron registros de ventas para {nombre_empresa} en este período.")
                    continue

                df_empresa = pd.DataFrame(lista_ventas)

                if not df_empresa.empty:
                    df_empresa['empresa'] = nombre_empresa
                    lista_dfs_empresas.append(df_empresa)
                    print(f"¡Éxito! Se extrajeron {len(df_empresa)} registros de ventas de {nombre_empresa}.")
            else:
                print(f"Advertencia: La respuesta de la API para {nombre_empresa} no tuvo el formato esperado (sin clave 'data')")
        except Exception as e:
            print(f"Error al procesar {nombre_empresa}: {e}")
            #Imprimimos más detalles si es un error de la API
            if hasattr(e, 'response') and e.response is not None:
                print(f"Respuesta del servidor: {e.response.text}")

    if not lista_dfs_empresas:
        print("Info: No se encontraron ventas para el periodo especificado.")
        return None
    
    df_consolidado = pd.concat(lista_dfs_empresas, ignore_index=True)
    # 1. Definir las columnas que queremos del API (las "llaves" del mapa)
    columnas_api_deseadas = list(mapeo_columnas_api.keys())
    # 2. Crear la lista final de columnas a MANTENER
    #    (Solo las que realmente existen en el DataFrame consolidado)
    columnas_a_mantener = [col for col in columnas_api_deseadas if col in df_consolidado.columns]
    # 3. Añadir la columna 'empresa' que creamos manualmente
    if 'empresa' in df_consolidado.columns:
        columnas_a_mantener.append('empresa')
    else:
        # Esto no debería pasar si el bucle funcionó, pero es una buena validación
        print("ADVERTENCIA: La columna 'empresa' no se encontró en el DataFrame consolidado.")
        
    # 4. FILTRAR el DataFrame ANTES de renombrar.
    #    Esto elimina columnas no deseadas (como la 'costo' original de la API)
    #    y previene el error de duplicados.
    df_filtrado = df_consolidado[columnas_a_mantener]
    
    # 5. AHORA, renombrar las columnas a sus nombres de DB
    df_final = df_filtrado.rename(columns=mapeo_columnas_api)
    
    print("Info: Transformando tipos de datos...")

    # 1. Columnas que deben ser INTEGER (Enteros)
    #    (Revisa esta lista. 'cant' es casi seguro el del error '3.00')
    columnas_integer = ['cant', 'porc_iva', 'factor', 'lista_precio'] 

    for col in columnas_integer:
        if col in df_final.columns:
            # Paso A: Convertir a numérico (float). 
            # 'errors=coerce' convierte textos no válidos (ej. "") en NaN (Nulo).
            # Esto convierte "3.00" (string) en 3.0 (float).
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
            # Paso B: Convertir de float a Entero Nulable ('Int64').
            # 'Int64' (con 'I' mayúscula) es el tipo de pandas que:
            # - Convierte 3.0 (float) en 3 (int).
            # - Maneja los NaN (Nulos) correctamente como <NA>.
            df_final[col] = df_final[col].astype('Int64')

    # 2. Columnas que deben ser NUMERIC (con decimales)
    #    (Es buena práctica hacer esto para todas las columnas de dinero/peso)
    columnas_float = [
        'valor_base', 'iva', 'descuento', 'valor', 'precio',
        'precio_mayor', 'peso_bruto', 'costo'
    ]
    
    for col in columnas_float:
        if col in df_final.columns:
            # Solo las convertimos a numérico (float) y dejamos que la BD maneje los nulos.
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # 3. Columnas que deben ser FECHA
    #    (¡Muy importante para tu 'DELETE' y para la consistencia!)
    if 'fecha' in df_final.columns:
        # Convierte los strings de fecha en objetos de fecha reales.
        # 'errors=coerce' convierte fechas inválidas en NaT (Nulo para fechas).
        #Le decimos a pandas que el string que viene de la api está en formato dd/mm/yyyy
        df_final['fecha'] = pd.to_datetime(df_final['fecha'], format="%d/%m/%Y", errors='coerce')

    # 4. Columnas que deben ser TEXTO (String)
    #    (Asegura que los códigos no se interpreten como números)
    columnas_string = [
        'nit', 'factura', 'codigo', 'cod_linea', 'cod_cliente', 'cod_vendedor'
    ]
    
    for col in columnas_string:
        if col in df_final.columns:
            # Convertimos nulos (NaN) a None y luego a string.
            df_final[col] = df_final[col].fillna(pd.NA).astype(str)
            # Reemplazamos el texto '<NA>' resultante por None (Nulo de Python)
            # que psycopg2 entiende como NULL de la base de datos.
            df_final[col] = df_final[col].replace('<NA>', None)


    print(f"\nInfo: Extracción y Transformación completada. {len(df_final)} registros procesados.")
    print(f"Info: {len(df_final.columns)} columnas preparadas para la carga.")
    
    # Devolver el DataFrame final, limpio, filtrado y TRANSFORMADO
    return df_final

def cargar_ventas_db(df_datos, conn, table_name, fecha_desde, fecha_hasta):
    """
    Paso 2: Carga (Versión BULK - Todas las empresas)
    Implementa la estrategia de 'Borrar y Cargar' para sincronizar los datos.
    Borra TODOS los registros del rango de fechas, sin filtrar por empresa.
    """
    print(f"\nINFO: Iniciando carga BULK en '{table_name}' para el rango {fecha_desde} a {fecha_hasta}...")
    
    if df_datos is None or df_datos.empty:
        print(f"ADVERTENCIA: No hay datos para cargar en '{table_name}'.")
        return

    try:
        # -----------------------------------------------------------------
        # Paso 1: Borrar los datos existentes
        # Borramos TODOS los registros en el rango de fechas,
        # sin importar la empresa, para reemplazarlos con el nuevo set.
        # Asunción: La tabla 'ventas_detalladas' tiene una columna 'fecha'.
        
        print(f"Info: Eliminando TODOS los datos de '{table_name}' entre {fecha_desde} y {fecha_hasta}...")
        
        delete_query = f"""
            DELETE FROM public."{table_name}"
            WHERE fecha BETWEEN %s AND %s; 
        """
        
        # Usamos la función genérica 'execute_query' que ya importaste
        execute_query(conn, delete_query, params=(fecha_desde, fecha_hasta))
        
        print(f"¡ÉXITO! Los datos del rango han sido eliminados de '{table_name}'.")
        # -----------------------------------------------------------------

        # Paso 2: Cargar los nuevos datos (Lógica de inserción)
        columnas_db = list(df_datos.columns)
        # 1. Forzamos todo el DataFrame a tipos de 'objeto' de Python.
        #    Esto convierte 'numpy.int64' (que da error) a 'int' de Python (que funciona).
        # 2. Usamos '.where(pd.notnull...)' para convertir TODOS los tipos de nulos
        #    (pd.NA, np.nan, NaT) a 'None', que psycopg2 entiende como 'NULL'.
        
        df_para_insertar = df_datos.astype(object).where(pd.notnull(df_datos), None)

        # 3. Convertimos el DataFrame limpio a una lista de listas.
        #    .values.tolist() ahora es seguro porque solo hay tipos nativos.
        lista_de_listas = df_para_insertar.values.tolist()
        
        # 4. Convertimos la lista de listas en una lista de tuplas.
        datos_para_insertar = [tuple(row) for row in lista_de_listas]
        
        query_insert = f"INSERT INTO public.\"{table_name}\" ({', '.join(f'\"{c}\"' for c in columnas_db)}) VALUES %s;"
        
        with conn.cursor() as cursor:
            extras.execute_values(cursor, query_insert, datos_para_insertar, page_size=1000)
            conn.commit()
            # rowcount puede no ser fiable con execute_values, usamos len()
            print(f"¡ÉXITO! Se han insertado {len(datos_para_insertar)} nuevos registros en '{table_name}'.")

    except Exception as e:
        print(f"ERROR CRÍTICO durante la carga en '{table_name}': {e}")
        conn.rollback() # Revertimos cualquier cambio si hay un error
        raise # Es buena idea relanzar el error para que la orquestación lo sepa

def ejecutar_fase_1(fecha_inicio_str, fecha_fin_str):
    """
    Orquesta la Fase 1: Extracción y Carga de Ventas API.
    Esta función es llamada por main.py
    """    
    print(f"\n=== INICIO FASE 1: EXTRACCIÓN Y CARGA DE VENTAS ({fecha_inicio_str} a {fecha_fin_str}) ===")

    # Por defecto, el script buscará las ventas desde ayer hasta hoy
    #fecha_fin = date.today()
    #fecha_inicio = fecha_fin - timedelta(days=1)
    #fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
    #fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')

    #fecha_fin_str= '2025-10-01'
    #fecha_inicio_str = '2025-10-01'


    # --- Orquestación del Proceso ---
    # 1. Extraer (BULK - Todas las empresas)
    # Tu función 'extraer_ventas_api' ya hace esto, está perfecta.
    df_ventas_crudo = extraer_ventas_api(fecha_inicio_str, fecha_fin_str)
    
    table_name = "ventas_detalladas"

    # 2. Transformar y Cargar (solo si la extracción fue exitosa)
    if df_ventas_crudo is not None and not df_ventas_crudo.empty:
        conn = get_db_connection()
        if conn:
            try:
                # ¡Llamamos a la función de carga corregida (sin empresa_nombre)!
                cargar_ventas_db(
                    df_ventas_crudo, 
                    conn, 
                    table_name, 
                    fecha_inicio_str, 
                    fecha_fin_str
                )
            except Exception as e:
                # Capturamos el error relanzado por cargar_ventas_db
                print(f"ERROR: El proceso de carga a la base de datos falló: {e}")
            finally:
                conn.close()
                print("\nConexión a la base de datos cerrada.")
    
    print("\n== Fin fase 1: extracción y carga de ventas ==")