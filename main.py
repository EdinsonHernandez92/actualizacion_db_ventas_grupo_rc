import sys
import os
import importlib.util  # Necesario para la carga dinámica de scripts (Fase 2)
from datetime import datetime

# --- Configuración del Proyecto ---
# Añadimos la ruta raíz del proyecto al path de python para poder importar nuestros módulos
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# --- Importación de Utilidades ---
# Asumimos que config.py y db_utils.py están correctos
try:
    from utils import user_inputs # Nuestras funciones para preguntar
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error: No se pudieron importar las utilidades. Revisa tu archivo .env y la estructura.")
    print(f"Detalle: {e}")
    sys.exit(1) # Salir si no podemos importar lo básico

# --- Importación de Módulos de Fases ---
try:
    from fase_1_extraccion_ventas.cargar_ventas_api import ejecutar_fase_1
    from fase_3_exporte_xlsx.export_to_xlsx import ejecutar_fase_3
    # Nota: La Fase 2 se importa dinámicamente, no aquí.
except ImportError as e:
    print(f"Error: No se pudo importar un módulo de fase. ¿Renombraste las funciones?")
    print(f"Detalle: {e}")
    sys.exit(1)

def mostrar_menu_principal():
    """Imprime el menú principal y retorna la opción del usuario."""
    print("\n" + "="*40)
    print("  SISTEMA DE ACTUALIZACIÓN DE VENTAS")
    print("="*40)
    print("[1] Ejecutar Flujo Completo (Fase 1 -> 2 -> 3)")
    print("[2] Ejecutar solo Fase 1: Extracción de API")
    print("[3] Ejecutar solo Fase 2: Ajustes de Base de Datos")
    print("[4] Ejecutar solo Fase 3: Exportar a Excel")
    print("[5] Salir")
    print("-"*40)
    return input("Elige una opción: ").strip()

def correr_fase_1():
    """Pide fechas y ejecuta la Fase 1."""
    try:
        fecha_ini, fecha_fin = user_inputs.pedir_rango_fechas()
        if fecha_ini and fecha_fin:
            ejecutar_fase_1(fecha_ini, fecha_fin)
    except Exception as e:
        print(f"ERROR INESPERADO en Fase 1: {e}")

def correr_fase_2():
    """Pide seleccionar un script y ejecuta la Fase 2."""
    # Ruta donde guardaste tus scripts 'ajustes_YYYY_MM.py'
    ruta_scripts = "fase_2_ajustes_db/scripts_del_mes/"
    
    script_path = user_inputs.seleccionar_script_ajuste(ruta_scripts)
    
    if not script_path:
        return # El usuario canceló o no hay scripts

    print(f"\n=== INICIO FASE 2: AJUSTES DE BASE DE DATOS ===")
    print(f"Ejecutando script: {os.path.basename(script_path)}")
    
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            print("ERROR: No se pudo obtener conexión a la BD para la Fase 2.")
            return

        # --- Magia de Importación Dinámica ---
        # 1. Creamos un "nombre de módulo" único basado en el archivo
        module_name = os.path.basename(script_path).replace('.py', '')
        
        # 2. Obtenemos las "especificaciones" del archivo
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        
        # 3. Creamos un módulo vacío basado en esas especificaciones
        script_module = importlib.util.module_from_spec(spec)
        
        # 4. "Ejecutamos" el archivo .py para cargar su contenido en nuestro módulo
        spec.loader.exec_module(script_module)
        # --- Fin de la Magia ---

        # 5. Ahora podemos llamar a la función estándar que definimos
        if hasattr(script_module, 'ejecutar_ajustes'):
            script_module.ejecutar_ajustes(conn) # Le pasamos la conexión
        else:
            print(f"ERROR: El script {script_path} no tiene una función 'ejecutar_ajustes(conn)'.")
            conn.rollback() # Aseguramos revertir si algo se hizo antes

    except Exception as e:
        print(f"ERROR INESPERADO durante la ejecución del script de ajuste: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Conexión a la base de datos cerrada.")
    
    print("\n== FIN FASE 2: Ajustes de Base de Datos ==\n")

def correr_fase_3():
    """Pide mes/año y ejecuta la Fase 3."""
    try:
        mes, anio = user_inputs.pedir_mes_anio_exporte()
        if mes and anio:
            ejecutar_fase_3(mes, anio)
    except Exception as e:
        print(f"ERROR INESPERADO en Fase 3: {e}")

def correr_flujo_completo():
    """Ejecuta las tres fases en secuencia, pidiendo los datos necesarios."""
    print("\n" + "#"*40)
    print("      INICIANDO FLUJO COMPLETO")
    print("#"*40)
    
    # --- PASO 1 ---
    print("\n--- PASO 1: EXTRACCIÓN API ---")
    try:
        fecha_ini, fecha_fin = user_inputs.pedir_rango_fechas()
        if not (fecha_ini and fecha_fin):
            print("Cancelado por el usuario. Abortando flujo.")
            return
        ejecutar_fase_1(fecha_ini, fecha_fin)
    except Exception as e:
        print(f"ERROR CRÍTICO en Fase 1. Abortando flujo: {e}")
        return # Detenemos el flujo si la Fase 1 falla

    # --- PASO 2 ---
    print("\n--- PASO 2: AJUSTES DB ---")
    try:
        correr_fase_2() # Esta función ya maneja la selección y ejecución
    except Exception as e:
        print(f"ERROR CRÍTICO en Fase 2. Abortando flujo: {e}")
        return # Detenemos si la Fase 2 falla

    # --- PASO 3 ---
    print("\n--- PASO 3: EXPORTE EXCEL ---")
    try:
        mes, anio = user_inputs.pedir_mes_anio_exporte()
        if not (mes and anio):
            print("Cancelado por el usuario. Abortando flujo.")
            return
        ejecutar_fase_3(mes, anio)
    except Exception as e:
        print(f"ERROR CRÍTICO en Fase 3: {e}")
        
    print("\n" + "#"*40)
    print("    ¡FLUJO COMPLETO TERMINADO!")
    print("#"*40 + "\n")


def main():
    """Bucle principal del programa."""
    while True:
        opcion = mostrar_menu_principal()
        
        if opcion == '1':
            correr_flujo_completo()
        
        elif opcion == '2':
            correr_fase_1()
            
        elif opcion == '3':
            correr_fase_2()
            
        elif opcion == '4':
            correr_fase_3()
            
        elif opcion == '5':
            print("Saliendo del programa. ¡Adiós!")
            break
            
        else:
            print("Opción no válida. Por favor, intente de nuevo.")

if __name__ == "__main__":
    main()