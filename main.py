import sys
import os
import importlib.util 
from datetime import datetime

# Añadimos la ruta raíz del proyecto al path de python para poder importar nuestros módulos
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# --- Importación de Utilidades ---
try:
    from utils import user_inputs 
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error: No se pudieron importar las utilidades. Revisa tu archivo .env y la estructura.")
    print(f"Detalle: {e}")
    sys.exit(1)

# --- Importación de Módulos de Fases ---
try:
    # FASE 1 - VENTAS (Renombrada)
    from fase_1_extraccion_ventas.cargar_ventas_api import ejecutar_fase_1 as ejecutar_fase_1_ventas 
    # FASE 1 - INVENTARIO
    from fase_1_extraccion_inventario.cargar_inventario_api import ejecutar_fase_1_inventario
    # FASE 1 - TERCEROS (¡NUEVO!)
    from fase_1_extraccion_terceros.cargar_terceros_api import ejecutar_fase_1_terceros
    # FASE 3
    from fase_3_exporte_xlsx.export_to_xlsx import ejecutar_fase_3
except ImportError as e:
    print(f"Error: No se pudo importar un módulo de fase. ¿Revisaste las rutas?")
    print(f"Detalle: {e}")
    sys.exit(1)


def mostrar_menu_principal():
    """Imprime el menú principal y retorna la opción del usuario."""
    print("\n" + "="*40)
    print("  SISTEMA DE ACTUALIZACIÓN DE VENTAS")
    print("="*40)
    print("--- 1. Flujos Completos ---")
    print("[1] Ejecutar Flujo de VENTAS Completo (API -> Ajustes -> Excel)")
    print("--- 2. Extracción y Carga (Fase 1) ---")
    print("[2] Ventas (Extracción API por Rango)")
    print("[3] Inventario (Actualización completa)")
    print("[4] Terceros (Actualización completa) <--- ¡NUEVO!")
    print("--- 3. Mantenimiento ---")
    print("[5] Ejecutar solo Fase 2: Ajustes de Base de Datos")
    print("[6] Ejecutar solo Fase 3: Exportar Ventas a Excel")
    print("[7] Salir")
    print("-"*40)
    return input("Elige una opción: ").strip()

def correr_fase_1_ventas():
    """Pide fechas y ejecuta la Fase 1 de Ventas."""
    try:
        fecha_ini, fecha_fin = user_inputs.pedir_rango_fechas()
        if fecha_ini and fecha_fin:
            ejecutar_fase_1_ventas(fecha_ini, fecha_fin)
    except Exception as e:
        print(f"ERROR INESPERADO en Fase 1 - Ventas: {e}")

def correr_fase_2():
    """Pide seleccionar un script y ejecuta la Fase 2 (Ajustes DB)."""
    ruta_scripts = "fase_2_ajustes_db/scripts_del_mes/"
    script_path = user_inputs.seleccionar_script_ajuste(ruta_scripts)
    
    if not script_path:
        return 

    print(f"\n=== INICIO FASE 2: AJUSTES DE BASE DE DATOS ===")
    print(f"Ejecutando script: {os.path.basename(script_path)}")
    
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            print("ERROR: No se pudo obtener conexión a la BD para la Fase 2.")
            return

        module_name = os.path.basename(script_path).replace('.py', '')
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        script_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script_module)

        if hasattr(script_module, 'ejecutar_ajustes'):
            script_module.ejecutar_ajustes(conn) 
        else:
            print(f"ERROR: El script {script_path} no tiene una función 'ejecutar_ajustes(conn)'.")
            conn.rollback() 

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
    """Pide mes/año y ejecuta la Fase 3 (Exporte Excel)."""
    try:
        mes, anio = user_inputs.pedir_mes_anio_exporte()
        if mes and anio:
            ejecutar_fase_3(mes, anio)
    except Exception as e:
        print(f"ERROR INESPERADO en Fase 3: {e}")

def correr_flujo_completo():
    """Ejecuta las tres fases de Ventas en secuencia."""
    print("\n" + "#"*40)
    print("      INICIANDO FLUJO COMPLETO DE VENTAS")
    print("#"*40)
    
    # PASO 1: Extracción de API (Ventas)
    print("\n--- PASO 1: EXTRACCIÓN API (VENTAS) ---")
    try:
        fecha_ini, fecha_fin = user_inputs.pedir_rango_fechas()
        if not (fecha_ini and fecha_fin): return
        ejecutar_fase_1_ventas(fecha_ini, fecha_fin)
    except Exception as e:
        print(f"ERROR CRÍTICO en Fase 1. Abortando flujo: {e}")
        return 

    # PASO 2: Ajustes DB (Ventas)
    print("\n--- PASO 2: AJUSTES DB (VENTAS) ---")
    try:
        correr_fase_2()
    except Exception as e:
        print(f"ERROR CRÍTICO en Fase 2. Abortando flujo: {e}")
        return 

    # PASO 3: Exporte Excel (Ventas)
    print("\n--- PASO 3: EXPORTE EXCEL (VENTAS) ---")
    try:
        mes, anio = user_inputs.pedir_mes_anio_exporte()
        if not (mes and anio): return
        ejecutar_fase_3(mes, anio)
    except Exception as e:
        print(f"ERROR CRÍTICO en Fase 3: {e}")
        
    print("\n" + "#"*40)
    print("  ¡FLUJO COMPLETO DE VENTAS TERMINADO!")
    print("#"*40 + "\n")


def main():
    """Bucle principal del programa."""
    while True:
        opcion = mostrar_menu_principal()
        
        if opcion == '1':
            correr_flujo_completo()
        
        elif opcion == '2':
            correr_fase_1_ventas()
            
        elif opcion == '3':
            ejecutar_fase_1_inventario()
            
        elif opcion == '4':
            ejecutar_fase_1_terceros() # <--- ¡NUEVA LLAMADA DIRECTA!
            
        elif opcion == '5':
            correr_fase_2()
            
        elif opcion == '6':
            correr_fase_3()
            
        elif opcion == '7':
            print("Saliendo del programa. ¡Adiós!")
            break
            
        else:
            print("Opción no válida. Por favor, intente de nuevo.")

if __name__ == "__main__":
    main()