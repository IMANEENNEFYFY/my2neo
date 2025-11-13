from flask import Flask, request, render_template, jsonify, redirect, url_for
import os
import threading
from .mysql_to_neo4j import (
    process_sql_to_neo4j,
    connect_mysql,
    get_tables,
    get_neo4j_graph_data,
    translate_sql_to_cypher,
    execute_sql_query,
    execute_cypher,
)

progress_data = {"percent": 0, "message": ""}
conversion_data = {"mysql_structure": {}, "neo4j_graph": {}}
# Stocke les dernières informations de connexion fournies par l'utilisateur
last_conn = {"mysql": {}, "neo4j": {}}

def create_app():
    template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
    app = Flask(__name__, template_folder=template_dir)

    UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "..", "uploads")
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

    @app.route("/", methods=["GET"])
    def index():
        return render_template("index.html")

    @app.route("/start_conversion", methods=["POST"])
    def start_conversion():
        file = request.files.get("sql_file")
        mysql_host = request.form.get("mysql_host")
        mysql_user = request.form.get("mysql_user")
        mysql_password = request.form.get("mysql_password")
        mysql_db = request.form.get("mysql_db")
        neo4j_uri = request.form.get("neo4j_uri")
        neo4j_user = request.form.get("neo4j_user")
        neo4j_password = request.form.get("neo4j_password")

        # Sauvegarder les infos de connexion pour usage ultérieur (outil de requête)
        last_conn["mysql"] = {"host": mysql_host, "user": mysql_user, "password": mysql_password, "db": mysql_db}
        last_conn["neo4j"] = {"uri": neo4j_uri, "user": neo4j_user, "password": neo4j_password}

        path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(path)

        # Lance conversion en thread pour ne pas bloquer le serveur
        def run_conversion():
            process_sql_to_neo4j(
                path, mysql_host, mysql_user, mysql_password, mysql_db,
                neo4j_uri, neo4j_user, neo4j_password, progress_data
            )

            # Récupérer structure MySQL après conversion
            conn = connect_mysql(mysql_host, mysql_user, mysql_password, mysql_db)
            tables = get_tables(conn)
            structure = {}
            for t in tables:
                cur = conn.cursor(dictionary=True)
                cur.execute(f"DESCRIBE `{t}`")
                structure[t] = cur.fetchall()
                cur.close()
            conn.close()

            # Récupérer graphe Neo4j
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            neo4j_graph = get_neo4j_graph_data(driver, mysql_db)
            driver.close()

            conversion_data["mysql_structure"] = structure
            conversion_data["neo4j_graph"] = neo4j_graph

        threading.Thread(target=run_conversion).start()

        # Redirige immédiatement vers la page de résultats
        return redirect(url_for("results"))

    @app.route("/results", methods=["GET"])
    def results():
        return render_template("result.html",
                               mysql_structure=conversion_data["mysql_structure"],
                               neo4j_graph=conversion_data["neo4j_graph"])

    @app.route('/query_tool', methods=['GET', 'POST'])
    def query_tool():
        sql = None
        sql_result = None
        sql_time = None
        cypher = None
        cypher_result = None
        cypher_time = None
        error = None

        if request.method == 'POST':
            sql = request.form.get('sql_query') or ''
            try:
                # Exécuter la requête SQL sur la base MySQL fournie précédemment
                mysql_host = request.form.get('mysql_host') or last_conn.get('mysql', {}).get('host')
                mysql_user = request.form.get('mysql_user') or last_conn.get('mysql', {}).get('user')
                mysql_password = request.form.get('mysql_password') or last_conn.get('mysql', {}).get('password')
                mysql_db = request.form.get('mysql_db') or last_conn.get('mysql', {}).get('db')

                conn = connect_mysql(mysql_host, mysql_user, mysql_password, mysql_db)
                sql_result, sql_time = execute_sql_query(conn, sql)
                conn.close()

                # Traduire (si possible) en Cypher
                cypher = translate_sql_to_cypher(sql)
                if cypher:
                    from neo4j import GraphDatabase
                    neo4j_uri = request.form.get('neo4j_uri') or last_conn.get('neo4j', {}).get('uri')
                    neo4j_user = request.form.get('neo4j_user') or last_conn.get('neo4j', {}).get('user')
                    neo4j_password = request.form.get('neo4j_password') or last_conn.get('neo4j', {}).get('password')
                    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
                    cypher_result, cypher_time = execute_cypher(driver, mysql_db, cypher)
                    driver.close()
                else:
                    cypher = "-- Conversion non prise en charge pour cette requête"
            except Exception as e:
                error = str(e)

        return render_template('query_tool.html',
                               sql=sql,
                               sql_result=sql_result,
                               sql_time=sql_time,
                               cypher=cypher,
                               cypher_result=cypher_result,
                               cypher_time=cypher_time,
                               error=error,
                               last_conn=last_conn)

    @app.route("/progress", methods=["GET"])
    def get_progress():
        return jsonify(progress_data)

    return app