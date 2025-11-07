# musqltoneo4j.py
import mysql.connector
from neo4j import GraphDatabase
from typing import Dict, Any

# ------------------- Helpers MySQL -------------------
def connect_mysql(host, user, password, db=None):
    cfg = {"host": host, "user": user, "password": password, "autocommit": True}
    if db:
        cfg["database"] = db
    return mysql.connector.connect(**cfg)

def get_tables(conn):
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    cur.close()
    return tables

def get_foreign_keys(conn, table):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND REFERENCED_TABLE_NAME IS NOT NULL
    """, (conn.database, table))
    fks = cur.fetchall()
    cur.close()
    return fks

def export_rows(conn, table):
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM `{table}`")
    for row in cur:
        yield row
    cur.close()

def label_for_table(table):
    return ''.join(part.capitalize() for part in table.split('_'))

# ------------------- Helpers Neo4j -------------------
def sanitize_db_name(name: str) -> str:
    """
    Transforme le nom en un nom Neo4j valide : uniquement lettres et chiffres
    """
    return ''.join(c for c in name if c.isalnum())

def ensure_neo4j_database(driver, db_name: str) -> str:
    valid_db_name = sanitize_db_name(db_name)
    with driver.session(database="system") as session:
        existing = [r["name"] for r in session.run("SHOW DATABASES")]
        if valid_db_name not in existing:
            session.run(f"CREATE DATABASE {valid_db_name}")
        
        # Attendre que la DB soit ONLINE
        while True:
            status = session.run(f"SHOW DATABASES YIELD name, currentStatus WHERE name='{valid_db_name}'").single()
            if status and status["currentStatus"] == "online":
                break
    return valid_db_name

def create_nodes(driver, table, rows, db_name):
    if not rows:
        return
    label = label_for_table(table)
    with driver.session(database=db_name) as session:
        props_list = [{k:v for k,v in r.items() if v is not None} for r in rows]
        pk = "id" if "id" in props_list[0] else list(props_list[0].keys())[0]
        cypher = f"""
        UNWIND $rows AS row
        MERGE (n:{label} {{ {pk}: row.{pk} }})
        SET n += row
        """
        session.run(cypher, {"rows": props_list})

def create_relationships(driver, table, fks, rows, db_name):
    if not fks or not rows:
        return
    with driver.session(database=db_name) as session:
        for fk in fks:
            fk_col = fk["COLUMN_NAME"]
            ref_table = fk["REFERENCED_TABLE_NAME"]
            ref_col = fk["REFERENCED_COLUMN_NAME"]
            label_this = label_for_table(table)
            label_ref = label_for_table(ref_table)
            cypher = f"""
            UNWIND $rows AS r
            MATCH (a:{label_this} {{ {fk_col}: r.{fk_col} }})
            MATCH (b:{label_ref} {{ {ref_col}: r.{fk_col} }})
            MERGE (a)-[:{table.upper()}_{fk_col.upper()}]->(b)
            """
            session.run(cypher, {"rows": rows})

# ------------------- Main Conversion -------------------
def process_sql_to_neo4j(sql_path, mysql_host, mysql_user, mysql_password,
                          mysql_db, neo4j_uri, neo4j_user, neo4j_password,
                          progress: Dict[str,Any]=None):
    try:
        if progress: progress.update({"percent":5,"message":"Connexion à MySQL..."})
        conn = connect_mysql(mysql_host, mysql_user, mysql_password)
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{mysql_db}`")
        cur.execute(f"CREATE DATABASE `{mysql_db}`")
        cur.execute(f"USE `{mysql_db}`")
        cur.close()
        if progress: progress.update({"percent":15,"message":f"Base MySQL `{mysql_db}` prête !"})

        # Import SQL
        with open(sql_path,"r",encoding="utf-8") as f:
            statements = [stmt for stmt in f.read().split(";") if stmt.strip()]
        conn = connect_mysql(mysql_host, mysql_user, mysql_password, mysql_db)
        cur = conn.cursor()
        total = len(statements)
        for i, stmt in enumerate(statements,1):
            cur.execute(stmt)
            if progress:
                progress.update({"percent":15+int(35*i/total),
                                 "message":f"Import SQL {i}/{total}"})
        cur.close()

        # Connect Neo4j
        if progress: progress.update({"percent":50,"message":"Connexion à Neo4j..."})
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

        # Crée une nouvelle base Neo4j
        neo_db = ensure_neo4j_database(driver, mysql_db)
        if progress: progress.update({"percent":55,"message":f"Base Neo4j `{neo_db}` prête !"})

        # Convert tables
        tables = get_tables(conn)
        total_tables = len(tables)
        for i, t in enumerate(tables,1):
            rows = list(export_rows(conn, t))
            create_nodes(driver, t, rows, neo_db)
            fks = get_foreign_keys(conn, t)
            create_relationships(driver, t, fks, rows, neo_db)
            if progress:
                progress.update({"percent":55+int(45*i/total_tables),
                                 "message":f"Conversion table {t} ({i}/{total_tables})"})

        conn.close()
        driver.close()
        if progress: progress.update({"percent":100,"message":"Conversion terminée avec succès !"})
    except Exception as e:
        if progress: progress.update({"percent":100,"message":str(e)})

# ------------------- CLI -------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 9:
        print("Usage: python musqltoneo4j.py <fichier_sql> <mysql_host> <mysql_user> <mysql_password> <mysql_db> <neo4j_uri> <neo4j_user> <neo4j_password>")
        sys.exit(1)

    sql_file = sys.argv[1]
    mysql_host, mysql_user, mysql_password, mysql_db, neo4j_uri, neo4j_user, neo4j_password = sys.argv[2:10]

    process_sql_to_neo4j(
        sql_file,
        mysql_host,
        mysql_user,
        mysql_password,
        mysql_db,
        neo4j_uri,
        neo4j_user,
        neo4j_password
    )
