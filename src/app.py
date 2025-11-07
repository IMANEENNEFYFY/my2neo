from flask import Flask, request, render_template_string, jsonify
import os
import threading
from mysql_to_neo4j import process_sql_to_neo4j

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

progress_data = {"percent": 0, "message": ""}

HTML_FORM = """
<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Convertisseur MySQL → Neo4j</title>
<style>
body { font-family: Arial,sans-serif; background:#f0f4f8; display:flex; justify-content:center; align-items:center; height:100vh; margin:0;}
.container { background:white; border-radius:15px; padding:30px 40px; width:500px; box-shadow:0 10px 30px rgba(0,0,0,0.1);}
h1 { text-align:center; color:#00796b; margin-bottom:25px; }
label { display:block; margin-top:10px; font-weight:600; color:#004d40; }
input { width:100%; padding:10px; margin-top:5px; border-radius:6px; border:1px solid #b2dfdb; }
button { margin-top:20px; width:100%; padding:12px; border:none; background:#00796b; color:white; font-weight:600; border-radius:8px; cursor:pointer; }
button:hover { background:#004d40; }
.progress { width:100%; background:#eee; border-radius:8px; margin-top:20px; }
.progress-bar { width:0%; height:20px; background:#4caf50; text-align:center; color:white; border-radius:8px; }
.message { margin-top:15px; white-space: pre-line; font-weight:bold; max-height:200px; overflow:auto; }
</style>
</head>
<body>
<div class="container">
<h1>Convertisseur MySQL → Neo4j</h1>
<form id="convertForm" enctype="multipart/form-data">
  <label>Fichier SQL :</label><input type="file" name="sql_file" required>
  <label>MySQL - Hôte :</label><input type="text" name="mysql_host" value="localhost" required>
  <label>MySQL - Utilisateur :</label><input type="text" name="mysql_user" value="root" required>
  <label>MySQL - Mot de passe :</label><input type="password" name="mysql_password">
  <label>MySQL - Base :</label><input type="text" name="mysql_db" required>
  <label>Neo4j - URI :</label><input type="text" name="neo4j_uri" value="bolt://localhost:7687" required>
  <label>Neo4j - Utilisateur :</label><input type="text" name="neo4j_user" value="neo4j" required>
  <label>Neo4j - Mot de passe :</label><input type="password" name="neo4j_password" required>
  <button type="submit">Lancer la conversion</button>
</form>
<div class="progress">
  <div class="progress-bar" id="progressBar">0%</div>
</div>
<div class="message" id="messageBox"></div>
</div>
<script>
const form = document.getElementById('convertForm');
const progressBar = document.getElementById('progressBar');
const messageBox = document.getElementById('messageBox');

form.addEventListener('submit', (e) => {
  e.preventDefault();
  messageBox.textContent = '';
  progressBar.style.width = '0%';
  progressBar.textContent = '0%';

  const formData = new FormData(form);

  fetch('/start_conversion', { method:'POST', body:formData })
    .then(() => {
      const interval = setInterval(async () => {
        const res = await fetch('/progress');
        const data = await res.json();
        progressBar.style.width = data.percent + '%';
        progressBar.textContent = data.percent + '%';
        messageBox.textContent = data.message;
        if (data.percent >= 100) clearInterval(interval);
      }, 500);
    });
});
</script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def index():
    return render_template_string(HTML_FORM)

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

    # Thread pour ne pas bloquer Flask
    threading.Thread(
        target=process_sql_to_neo4j,
        args=(path, mysql_host, mysql_user, mysql_password,
              mysql_db, neo4j_uri, neo4j_user, neo4j_password, progress_data)
    ).start()

    return '', 204

@app.route("/progress", methods=["GET"])
def get_progress():
    return progress_data

if __name__ == "__main__":
    app.run(debug=True)
