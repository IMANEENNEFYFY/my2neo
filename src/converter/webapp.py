from flask import Flask, request, render_template, jsonify, redirect, url_for
import os
import threading
from .mysql_to_neo4j import process_sql_to_neo4j, connect_mysql, get_tables, get_neo4j_graph_data

progress_data = {"percent": 0, "message": ""}
conversion_data = {"mysql_structure": {}, "neo4j_graph": {}}

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

    @app.route("/progress", methods=["GET"])
    def get_progress():
        return jsonify(progress_data)

    return app
