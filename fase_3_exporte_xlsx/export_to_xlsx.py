from sqlalchemy import create_engine
import pandas as pd
import sys
import os
import calendar
import locale
from datetime import date

# Añadimos la ruta raíz del proyecto al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config # Importamos nuestras configuraciones

def get_db_engine():
    """
    Crea y retorna un engine de SQLAlchemy a partir de config.db_config.
    """
    try:
        db = config.db_config
        if not db.get("password"):
            raise ValueError("La contraseña de la BD no está en config.py")
        
        db_url = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
        return create_engine(db_url)
    except Exception as e:
        print(f"Error al crear el engine de SQLAlchemy: {e}")
        return None

def get_month_details(mes, anio):
    """
    Calcula el primer día, último día y nombre en español del mes.
    """
    # Establecer el idioma a español para el nombre del mes
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_TIME, 'Spanish_Spain')
        except locale.Error:
            print("Advertencia: No se pudo establecer el idioma a español. Se usará el idioma por defecto.")
            
    # Calcular primer día
    fecha_inicio = date(anio, mes, 1).strftime('%Y-%m-%d')
    
    # Calcular último día
    ultimo_dia_num = calendar.monthrange(anio, mes)[1]
    fecha_fin = date(anio, mes, ultimo_dia_num).strftime('%Y-%m-%d')
    
    # Obtener nombre del mes
    nombre_mes = date(anio, mes, 1).strftime('%B').capitalize()
    
    return fecha_inicio, fecha_fin, nombre_mes, str(mes).zfill(2)

def ejecutar_fase_3(mes, anio):
    """
    Orquesta la Fase 3: Exportación de datos a Excel.
    Esta función es llamada por main.py
    """
    print(f"\n=== INICIO FASE 3: EXPORTACIÓN A EXCEL (Mes: {mes}, Año: {anio}) ===")

    # 1. Validar que las rutas de exportación existan en config
    if not hasattr(config, 'rutas_exportacion') or not config.rutas_exportacion:
        print("ERROR: 'rutas_exportacion' no está definido en config.py")
        print("Por favor, añade un diccionario 'rutas_exportacion' a tu config.py")
        return

    # 2. Obtener detalles del mes y motor de BD
    fecha_inicio, fecha_fin, nombre_mes, mes_num_str = get_month_details(mes, anio)
    engine = get_db_engine()
    
    if not engine:
        print("ERROR: No se pudo conectar a la base de datos. Abortando exportación.")
        return

    print(f"Exportando rango: {fecha_inicio} al {fecha_fin} (Mes: {nombre_mes})")

    try:
        # 3. Iterar sobre las plantillas de ruta en config
        for empresa, plantilla_ruta in config.rutas_exportacion.items():
            
            # 4. Crear la ruta de archivo final
            try:
                ruta_archivo = plantilla_ruta.format(
                    mes_num=mes_num_str,
                    mes_nombre=nombre_mes,
                    anio=anio
                )
            except KeyError as e:
                print(f"ADVERTENCIA: La plantilla de ruta para '{empresa}' tiene una clave desconocida: {e}")
                print(f"Plantilla: {plantilla_ruta}")
                continue # Saltar a la siguiente empresa

            # 5. Asegurarse de que el directorio de destino exista
            directorio_destino = os.path.dirname(ruta_archivo)
            if not os.path.exists(directorio_destino):
                os.makedirs(directorio_destino)
                print(f"Info: Se ha creado el directorio: {directorio_destino}")

            # --- ¡CORRECCIÓN DE SEGURIDAD! ---
            # Usamos una consulta parametrizada en lugar de f-strings
            # para prevenir Inyección SQL.
            query = """
                SELECT * FROM ventas_detalladas 
                WHERE empresa = %(empresa_param)s 
                AND fecha BETWEEN %(inicio_param)s AND %(fin_param)s
            """
            params = {
                "empresa_param": empresa,
                "inicio_param": fecha_inicio,
                "fin_param": fecha_fin
            }
            
            print(f"\nConsultando datos para: {empresa}...")
            df = pd.read_sql(query, engine, params=params)

            if not df.empty:
                # 7. Exportar a Excel
                df.to_excel(ruta_archivo, index=False)
                print(f"¡ÉXITO! Datos de {empresa} exportados a {ruta_archivo}")
            else:
                print(f"Info: No hay datos para exportar de {empresa} en el rango seleccionado.")

    except Exception as e:
        print(f"ERROR Inesperado durante la exportación: {e}")
    
    print("\n== FIN FASE 3: Exportación a Excel ==\n")