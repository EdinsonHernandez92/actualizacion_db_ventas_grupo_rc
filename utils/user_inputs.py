import os
from datetime import datetime

def pedir_rango_fechas():
    """
    Pide al usuario una fecha de inicio y fin, y las valida.
    Retorna (fecha_inicio_str, fecha_fin_str) en formato YYYY-MM-DD.
    """
    print("\n--- Definición de Rango de Fechas ---")
    while True:
        fecha_inicio_str = input("Ingrese Fecha Inicial (YYYY-MM-DD): ").strip()
        try:
            datetime.strptime(fecha_inicio_str, '%Y-%m-%d')
            break # Fecha válida, salimos del bucle
        except ValueError:
            print("Formato incorrecto. Por favor, use YYYY-MM-DD.")

    while True:
        fecha_fin_str = input("Ingrese Fecha Final   (YYYY-MM-DD): ").strip()
        try:
            fecha_fin_obj = datetime.strptime(fecha_fin_str, '%Y-%m-%d')
            fecha_inicio_obj = datetime.strptime(fecha_inicio_str, '%Y-%m-%d')
            
            if fecha_fin_obj < fecha_inicio_obj:
                print("Error: La fecha final no puede ser anterior a la fecha inicial.")
            else:
                break # Fecha válida y lógica, salimos
        except ValueError:
            print("Formato incorrecto. Por favor, use YYYY-MM-DD.")
            
    return fecha_inicio_str, fecha_fin_str

def pedir_mes_anio_exporte():
    """
    Pide al usuario un mes y año para la exportación y los valida.
    Retorna (mes, anio) como números enteros.
    """
    print("\n--- Definición de Mes de Exporte ---")
    current_year = datetime.now().year
    
    while True:
        try:
            anio_str = input(f"Ingrese el Año (ej. {current_year}): ").strip()
            anio = int(anio_str)
            if 2020 <= anio <= current_year + 5: # Un rango razonable
                break
            else:
                print(f"Error: Por favor ingrese un año razonable (ej. 2020-{current_year+5}).")
        except ValueError:
            print("Error: Ingrese un número válido para el año.")

    while True:
        try:
            mes_str = input("Ingrese el Mes (1-12): ").strip()
            mes = int(mes_str)
            if 1 <= mes <= 12:
                break
            else:
                print("Error: Por favor ingrese un número del 1 al 12.")
        except ValueError:
            print("Error: Ingrese un número válido para el mes.")
            
    return mes, anio

def seleccionar_script_ajuste(ruta_directorio):
    """
    Escanea un directorio en busca de scripts .py, los muestra
    y pide al usuario que seleccione uno.
    Retorna la ruta completa al script seleccionado, o None si cancela.
    """
    print("\n--- Selección de Script de Ajuste ---")
    
    if not os.path.exists(ruta_directorio):
        print(f"ERROR: El directorio de scripts no existe: {ruta_directorio}")
        return None

    # Encontrar solo archivos .py que NO sean __init__.py
    scripts = [f for f in os.listdir(ruta_directorio) if f.endswith('.py') and not f.startswith('__')]
    
    if not scripts:
        print(f"No se encontraron scripts de ajuste en: {ruta_directorio}")
        return None

    print("Scripts de ajuste disponibles:")
    for i, script in enumerate(scripts):
        print(f"  [{i + 1}] {script}")
    print("  [0] Omitir este paso / Cancelar")

    while True:
        try:
            opcion_str = input("Seleccione el script a ejecutar (número): ").strip()
            opcion = int(opcion_str)
            
            if 0 < opcion <= len(scripts):
                # El usuario seleccionó un script
                script_seleccionado = scripts[opcion - 1]
                return os.path.join(ruta_directorio, script_seleccionado)
            elif opcion == 0:
                print("Info: Se omitió la ejecución de scripts de ajuste.")
                return None
            else:
                print(f"Opción inválida. Ingrese un número entre 0 y {len(scripts)}.")
        except ValueError:
            print("Error: Ingrese solo el número de la opción.")