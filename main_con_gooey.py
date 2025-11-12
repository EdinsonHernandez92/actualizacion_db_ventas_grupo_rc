import sys
import os
import importlib.util
from datetime import datetime
import argparse # ¡Importamos argparse!
from gooey import Gooey, GooeyParser # ¡Importamos Gooey!

# --- Importación de Utilidades ---
# (Asumimos que config.py y db_utils.py están correctos)
try:
    from utils.db_utils import get_db_connection
except ImportError as e:
    print(f"Error: No se pudieron importar las utilidades. {e}")
    sys.exit(1)

# --- Importación de Módulos de Fases ---
try:
    from fase_1_extraccion_ventas.cargar_ventas_api import ejecutar_fase_1
    from fase_3_exporte_xlsx.export_to_xlsx import ejecutar_fase_3
except ImportError as e:
    print(f"Error: No se pudo importar un módulo de fase. {e}")
    sys.exit(1)

# --- LÓGICA DE FASE 2 (Adaptada para Gooey) ---
# Copiamos la lógica que estaba en el 'main.py' anterior,
# pero ahora acepta 'script_path' como argumento.
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

        # --- Magia de Importación Dinámica ---
        module_name = os.path.basename(script_path).replace('.py', '')
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        script_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script_module)
        # --- Fin de la Magia ---

        if hasattr(script_module, 'ejecutar_ajustes'):
            script_module.ejecutar_ajustes(conn) # Le pasamos la conexión
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


# --- ¡AQUÍ EMPIEZA LA MAGIA DE GOOEY! ---

# @Gooey es el decorador que transforma argparse en una GUI.
# El 'program_name' es el título de la ventana.
@Gooey(
    program_name="Sistema de Actualización de Ventas",
    default_size=(720, 700), # Tamaño de la ventana
    navigation='TABBED',    # ¡Usa pestañas para cada Fase!
    progress_regex=r"^Info: (.*)$" # Expresión para la barra de progreso
)
def main():
    """
    Función principal que define la interfaz de Gooey.
    """
    
    # 1. Creamos el "parser" principal
    parser = GooeyParser(description="Seleccione la tarea a ejecutar.")
    
    # 2. Creamos los "subparsers". Gooey los convertirá en Pestañas (Tabs)
    subparsers = parser.add_subparsers(dest='command', help="Seleccione una fase")

    # --- PESTAÑA 1: FLUJO COMPLETO ---
    full_parser = subparsers.add_parser(
        'completo', 
        help="Ejecuta las 3 fases en orden: API -> Ajustes -> Excel"
    )
    full_group = full_parser.add_argument_group("Opciones del Flujo Completo", gooey_options={'columns': 2})
    
    # Argumentos para la Fase 1 (Extracción)
    full_group.add_argument(
        '--fecha_inicio_f1', 
        help="Fecha inicial para extraer de la API (YYYY-MM-DD)",
        widget="DateChooser", # ¡Esto crea un calendario!
        gooey_options={'format': '%Y-%m-%d'} #Forzamos el formato de salida de la fecha
    )
    full_group.add_argument(
        '--fecha_fin_f1',
        help="Fecha final para extraer de la API (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )
    
    # Argumento para la Fase 2 (Ajustes)
    full_group.add_argument(
        '--script_ajuste_f2',
        help="Script de ajuste (.py) a ejecutar (Opcional)",
        widget="FileChooser" # ¡Esto crea un botón de "Buscar archivo"!
    )
    
    # Argumentos para la Fase 3 (Exporte)
    full_group.add_argument(
        '--mes_exporte_f3',
        help="Mes a exportar (1-12)",
        type=int,
        choices=list(range(1, 13)), # ¡Esto crea un dropdown!
        widget="Dropdown"
    )
    full_group.add_argument(
        '--anio_exporte_f3',
        help=f"Año a exportar (ej. {datetime.now().year})",
        type=int,
        default=datetime.now().year
    )

    # --- PESTAÑA 2: SOLO FASE 1 (Extracción API) ---
    fase1_parser = subparsers.add_parser(
        'fase1', 
        help="Ejecutar solo la extracción y carga desde la API"
    )
    fase1_parser.add_argument(
        'fecha_inicio',
        help="Fecha inicial para extraer de la API (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )
    fase1_parser.add_argument(
        'fecha_fin',
        help="Fecha final para extraer de la API (YYYY-MM-DD)",
        widget="DateChooser",
        gooey_options={'format': '%Y-%m-%d'}
    )
    
    # --- PESTAÑA 3: SOLO FASE 2 (Ajustes DB) ---
    fase2_parser = subparsers.add_parser(
        'fase2', 
        help="Ejecutar solo un script de ajuste en la BD"
    )
    fase2_parser.add_argument(
        'script_path',
        help="Seleccione el script de ajuste (.py) a ejecutar",
        widget="FileChooser", # Botón "Buscar archivo..."
        gooey_options={
            'wildcard': "Scripts de Python (*.py)|*.py"
        }
    )
    
    # --- PESTAÑA 4: SOLO FASE 3 (Exporte Excel) ---
    fase3_parser = subparsers.add_parser(
        'fase3', 
        help="Ejecutar solo la exportación a Excel"
    )
    fase3_parser.add_argument(
        'mes',
        help="Mes a exportar (1-12)",
        type=int,
        choices=list(range(1, 13)), # Dropdown
        widget="Dropdown"
    )
    fase3_parser.add_argument(
        'anio',
        help=f"Año a exportar (ej. {datetime.now().year})",
        type=int,
        default=datetime.now().year
    )
    
    # --- FIN DE LA DEFINICIÓN DE LA GUI ---

    # 3. Gooey parsea los argumentos que el usuario puso en la GUI
    args = parser.parse_args()

    # 4. Lógica para decidir QUÉ ejecutar (¡ya no hay bucle while!)
    
    print(f"Comando seleccionado: {args.command}")

    if args.command == 'completo':
        print("#"*40)
        print("      INICIANDO FLUJO COMPLETO")
        print("#"*40)
        
        # 1. Correr Fase 1 (¡Pasando los argumentos directamente!)
        # (Asegúrate de que tus DateChooser tengan el 'gooey_options' que pusimos primero)
        ejecutar_fase_1(args.fecha_inicio_f1, args.fecha_fin_f1)
        
        # 2. Correr Fase 2
        correr_fase_2_gooey(args.script_ajuste_f2)
        
        # 3. Correr Fase 3
        ejecutar_fase_3(args.mes_exporte_f3, args.anio_exporte_f3)
        
        print("\n¡FLUJO COMPLETO TERMINADO!")
        
    elif args.command == 'fase1':
        # ¡Pasamos los argumentos directamente!
        ejecutar_fase_1(args.fecha_inicio, args.fecha_fin)
        
    elif args.command == 'fase2':
        correr_fase_2_gooey(args.script_path)
        
    elif args.command == 'fase3':
        ejecutar_fase_3(args.mes, args.anio)
    
    else:
        print("No se seleccionó ningún comando. (Deberías ver esto en la terminal, no en Gooey)")

# --- FIN DEL SCRIPT ---

if __name__ == "__main__":
    # ¡Asegúrate de que la ruta raíz esté en el path!
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    main()