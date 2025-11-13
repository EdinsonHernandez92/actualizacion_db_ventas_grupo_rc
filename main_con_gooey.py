import sys
import os
import importlib.util 
from datetime import datetime
import argparse
from gooey import Gooey, GooeyParser

# Añadimos la ruta raíz del proyecto al path de python
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# --- Importación de Utilidades ---
try:
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error: No se pudieron importar las utilidades. {e}")
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


# --- LÓGICA DE FASE 2 (Adaptada para Gooey) ---
def correr_fase_2_gooey(script_path):
    """
    Recibe la ruta a un script de ajuste, lo importa dinámicamente
    y ejecuta su función 'ejecutar_ajustes(conn)'.
    """
    if not script_path or not os.path.exists(script_path):
        print("Info: No se seleccionó un script de ajuste o la ruta no es válida. Omitiendo Fase 2.")
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


# --- DEFINICIÓN DE LA GUI CON GOOEY ---

@Gooey(
    program_name="Sistema de Actualización de Datos ERP",
    default_size=(800, 700),
    navigation='TABBED',
    progress_regex=r"^Info: (.*)$"
)
def main():
    """
    Función principal que define la interfaz de Gooey.
    """
    
    parser = GooeyParser(description="Seleccione la tarea a ejecutar.")
    subparsers = parser.add_subparsers(dest='command', help="Seleccione una fase")

    # --- PESTAÑA 1: FLUJO COMPLETO (VENTAS) ---
    full_parser = subparsers.add_parser(
        'ventas_completo', 
        help="Ejecuta las 3 fases de Ventas en orden: API -> Ajustes -> Excel"
    )
    full_group = full_parser.add_argument_group("Opciones del Flujo Completo", gooey_options={'columns': 2})
    
    # Argumentos para la Fase 1 (Extracción de Ventas)
    full_group.add_argument(
        '--fecha_inicio_f1', 
        help="Fecha inicial para extraer ventas (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )
    full_group.add_argument(
        '--fecha_fin_f1',
        help="Fecha final para extraer ventas (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )
    
    # Argumento para la Fase 2 (Ajustes)
    full_group.add_argument(
        '--script_ajuste_f2',
        help="Script de ajuste (.py) a ejecutar (Opcional)",
        widget="FileChooser"
    )
    
    # Argumentos para la Fase 3 (Exporte)
    full_group.add_argument(
        '--mes_exporte_f3',
        help="Mes a exportar (1-12)",
        type=int,
        choices=list(range(1, 13)),
        widget="Dropdown"
    )
    full_group.add_argument(
        '--anio_exporte_f3',
        help=f"Año a exportar (ej. {datetime.now().year})",
        type=int,
        default=datetime.now().year
    )

    # --- PESTAÑA 2: SOLO FASE 1 - VENTAS ---
    fase1_ventas_parser = subparsers.add_parser(
        'fase1_ventas', 
        help="Extracción y carga de Ventas (por rango)"
    )
    fase1_ventas_parser.add_argument(
        'fecha_inicio',
        help="Fecha inicial para extraer ventas (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )
    fase1_ventas_parser.add_argument(
        'fecha_fin',
        help="Fecha final para extraer ventas (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )

    # --- PESTAÑA 3: SOLO FASE 1 - INVENTARIO ---
    fase1_inv_parser = subparsers.add_parser(
        'fase1_inventario',
        help="Actualización completa de Inventario (Upsert)"
    )
    fase1_inv_parser.add_argument(
        '--run_inventory',
        help="Presione Start para iniciar la actualización de inventario",
        action='store_true',
        default=True,
        widget="Block" # Hace que el argumento se vea como un simple bloque
    )
    
    # --- PESTAÑA 4: SOLO FASE 1 - TERCEROS (¡NUEVA PESTAÑA!) ---
    fase1_terceros_parser = subparsers.add_parser(
        'fase1_terceros',
        help="Actualización completa de Terceros (Upsert)"
    )
    fase1_terceros_parser.add_argument(
        '--run_terceros',
        help="Presione Start para iniciar la actualización de terceros",
        action='store_true',
        default=True,
        widget="Block"
    )

    # --- PESTAÑA 5: SOLO FASE 2 (Ajustes DB) ---
    fase2_parser = subparsers.add_parser(
        'fase2', 
        help="Ejecutar script de ajustes mensuales en la BD"
    )
    fase2_parser.add_argument(
        'script_path',
        help="Seleccione el script de ajuste (.py) a ejecutar",
        widget="FileChooser",
        gooey_options={
            'wildcard': "Scripts de Python (*.py)|*.py"
        }
    )
    
    # --- PESTAÑA 6: SOLO FASE 3 (Exporte Excel) ---
    fase3_parser = subparsers.add_parser(
        'fase3', 
        help="Exportación de Ventas a Excel"
    )
    fase3_parser.add_argument(
        'mes',
        help="Mes a exportar (1-12)",
        type=int,
        choices=list(range(1, 13)),
        widget="Dropdown"
    )
    fase3_parser.add_argument(
        'anio',
        help=f"Año a exportar (ej. {datetime.now().year})",
        type=int,
        default=datetime.now().year
    )
    
    # 3. Gooey parsea los argumentos
    args = parser.parse_args()

    # 4. Lógica para decidir QUÉ ejecutar
    
    print(f"Comando seleccionado: {args.command}")

    if args.command == 'ventas_completo':
        print("\n" + "#"*40)
        print("      INICIANDO FLUJO COMPLETO DE VENTAS")
        print("#"*40)
        
        # Corrección de fecha y ejecución de FASE 1
        fecha_inicio_obj = datetime.strptime(args.fecha_inicio_f1, '%Y-%m-%d')
        fecha_fin_obj = datetime.strptime(args.fecha_fin_f1, '%Y-%m-%d')
        fecha_ini_corregida = fecha_inicio_obj.strftime('%Y-%m-%d')
        fecha_fin_corregida = fecha_fin_obj.strftime('%Y-%m-%d')
        
        ejecutar_fase_1_ventas(fecha_ini_corregida, fecha_fin_corregida)
        correr_fase_2_gooey(args.script_ajuste_f2)
        ejecutar_fase_3(args.mes_exporte_f3, args.anio_exporte_f3)
        
        print("\n¡FLUJO COMPLETO TERMINADO!")
        
    elif args.command == 'fase1_ventas':
        # Corrección de fecha y ejecución
        fecha_inicio_obj = datetime.strptime(args.fecha_inicio, '%Y-%m-%d')
        fecha_fin_obj = datetime.strptime(args.fecha_fin, '%Y-%m-%d')
        fecha_ini_corregida = fecha_inicio_obj.strftime('%Y-%m-%d')
        fecha_fin_corregida = fecha_fin_obj.strftime('%Y-%m-%d')
        
        ejecutar_fase_1_ventas(fecha_ini_corregida, fecha_fin_corregida)
        
    elif args.command == 'fase1_inventario':
        ejecutar_fase_1_inventario()
        
    elif args.command == 'fase1_terceros': # <--- ¡NUEVA LÓGICA DE EJECUCIÓN!
        ejecutar_fase_1_terceros()
        
    elif args.command == 'fase2':
        correr_fase_2_gooey(args.script_path)
        
    elif args.command == 'fase3':
        ejecutar_fase_3(args.mes, args.anio)
    
    else:
        print("No se seleccionó ningún comando.")

# --- FIN DEL SCRIPT ---

if __name__ == "__main__":
    # Asegúrate de que la ruta raíz esté en el path (ya lo hicimos arriba)
    main()