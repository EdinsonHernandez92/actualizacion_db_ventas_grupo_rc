# ¡Ya no importamos psycopg2 ni nos conectamos aquí!

def ejecutar_ajustes(conn):
    """
    Aplica los ajustes de SQL de Octubre 2025.
    La conexión 'conn' es proporcionada por main.py.
    """
    print("-> Aplicando ajustes de Octubre 2025 (Solo SQLs)...")
    
    # Lista de sentencias SQL a ejecutar
    sql_statements = [
        # Errores en devoluciones
        """UPDATE ventas_detalladas SET cod_vendedor = 'V10', nom_vendedor = 'DUARTE MORALES LEIDY VIVIANA (SSM1', supervisor = 'SUP. SSM P1' WHERE factura = 'DVFECA1143076';""",
        # ... (todas tus otras sentencias UPDATE y DELETE) ...
        """DELETE FROM ventas_detalladas WHERE factura IN (
            'FVFECA1341379', 'FVFECA1341380', 'FVFECA1341381', 'FVFECA1341382',
            'FVFECA1341383', 'FVFECA1341384', 'FVFECA1341385', 'FVFECA1341386', 'FVFECA1341387',
            'DVFECA1143163', 'DVFECA1143164', 'DVFECA1143165', 'DVFECA1143168',
            'DVFECA1143170', 'DVFECA1143171', 'DVFECA1143173', 'DVFECA1143175', 'DVFECA1143179'
        );""",
    ]

    # Usamos la conexión que nos pasaron
    with conn.cursor() as cur:
        try:
            print(f"Ejecutando {len(sql_statements)} sentencias SQL...")
            for stmt in sql_statements:
                cur.execute(stmt)
            
            conn.commit() # Hacemos commit de la transacción
            print("✅ Correcciones de Octubre aplicadas exitosamente.")
        
        except Exception as e:
            print(f"ERROR al aplicar ajustes de Octubre: {e}")
            conn.rollback() # Revertimos si algo falla
            raise # Relanzamos el error para que main.py lo sepa