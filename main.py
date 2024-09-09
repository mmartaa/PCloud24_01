import os
import pandas as pd
from datetime import datetime
import tempfile

from functools import wraps
from flask import Flask, request, redirect, url_for, send_from_directory, jsonify, render_template, abort
from flask_login import LoginManager, current_user, login_user, logout_user, login_required, UserMixin
import json
from secret import secret_key
from google.cloud import firestore
from google.cloud import storage


class User(UserMixin): #classe utente che rappresenta gli utenti del sistema
    def __init__(self, username, is_admin=False):
        super().__init__()
        self.id = username
        self.username = username
        #self.is_admin


app = Flask(__name__)
app.config['SECRET_KEY'] = secret_key


login = LoginManager(app)
login.login_view = '/static/login.html'


db = 'livelyageing'

#creo client per accedere a database firestore
db = firestore.Client.from_service_account_json('credentials.json', database=db)
#client per accedere a cloud storage
storage_client = storage.Client.from_service_account_json('credentials.json')


admindb ={
    'marta':'gabbi'
}

usersdb = {
    'Carla':'carla',
    'Francesco':'francesco',
    'Lalla':'lalla',
    'Luigi':'luigi',
    'Luciano':'luciano'
}
'''
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_admin:  # Controlla se l'utente è admin
            abort(403)  # Accesso negato
        return f(*args, **kwargs)
    return decorated_function
'''

@app.route('/', methods=['GET', 'POST'])
def root():
    return redirect(url_for('static', filename='index.html'))


@login.user_loader
def load_user(username):
    if username in usersdb:
        return User(username)
    return None


@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        if current_user.is_authenticated:
            return redirect(url_for('data'))

        username = request.values['u']
        password = request.values['p']

        if username in usersdb and password == usersdb[username]:
            login_user(User(username), remember=True)
            return redirect(url_for('data'))
        print("login utente fallito")

        if username in admindb and password == admindb[username]:
            login_user(User(username), remember=True)
            print(current_user.username)
            return redirect(url_for('static', filename='main_admin.html'))
        print("login admin fallito")

    return redirect('/static/login.html')


@app.route('/logout', methods=["GET", "POST"])
@login_required
def logout():
    logout_user()
    return redirect('/')


#@app.route('/utenti',methods=['GET'])
# @admin_required

#@app.route('/utenti', methods=['GET'])
#def utenti():
#    return jsonify(list(usersdb.keys())), 200


@app.route('/utenti/<username>', methods=['GET'])
def get_user_graph(username):
    collection_ref = db.collection(f'dati_{username}')
    docs = collection_ref.stream()

    data = []
    for doc in docs:
        doc_data = doc.to_dict()
        data.append({
            'X': doc_data['X'],
            'Z': doc_data['Z'],
            'Tempo': doc_data['Tempo']
        })

    return jsonify(data)

'''
# Pagina HTML statica
@app.route('/prova/<username>', methods=['GET'])
@login_required
def prova(username):
    #return redirect(url_for('static', filename='user_chart2.html'))
    return send_from_directory('static', 'user_chart2.html')
'''

@app.route('/grafico', methods=['GET'])
@login_required
def grafico():
    username = current_user.username
    print(current_user.username)

    collection_ref = db.collection(f'dati_%s'%username) # il % fa da segnaposto
    docs = collection_ref.stream()

    # Process the data
    data = []
    for doc in docs:
        doc_data = doc.to_dict()
        data.append({
            'X': doc_data['X'],
            'Z': doc_data['Z'],
            'Tempo': doc_data['Tempo']
        })

    return jsonify(data)

@app.route('/data', methods=['GET'])
@login_required
def data():
    return redirect(url_for('static', filename='grafico.html'))



# CARICA FILE SU CLOUD STORAGE E MEMORIZZA SU FIRESTORE

def upload_to_cloud_storage(file_path, bucket_name):
    # Carica file su Google Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"File {file_path} uploaded to {bucket_name}/{blob_name}.")
    return blob

def parse_csv_and_store_in_firestore(blob, collection_name):
    # Prendi i dati CSV da Cloud Storage e memorizzali in Firestore
    # Download contenuto del blob

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file: #crea file temporaneo
        temp_filename = temp_file.name

    try:
        # Download il contenuto del blob nel file temporaneo
        blob.download_to_filename(temp_filename)

        # Analizza CSV
        df = pd.read_csv(temp_filename, sep=';')

        # converti il file in lista di dizionari
        records = df.to_dict('records')

        # Memorizza in Firestore
        for record in records:
            doc_id = f"{collection_name}_{str(record['Tempo']).replace(' ', '_').replace(':', '-')}"
            doc_ref = db.collection(collection_name).document(doc_id)
            doc_ref.set(record)

        print(f"Data from {blob.name} stored in Firestore collection {collection_name}")

    finally:
        # Pulisci file temporaneo
        os.unlink(temp_filename)

def process_csv_files(local_directory, bucket_name, collection_prefix):
    # Elabora tutti i file CSV in una cartella
    for filename in os.listdir(local_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(local_directory, filename)

            # Carica su Cloud Storage
            blob = upload_to_cloud_storage(file_path, bucket_name)

            # Analizza e memorizza in Firestore
            collection_name = f"{collection_prefix}_{os.path.splitext(filename)[0]}"
            parse_csv_and_store_in_firestore(blob, collection_name)


if __name__ == '__main__':
    # carica i dati prima di far partire app Flask
    local_directory = 'Dati'  # local directory con i file
    bucket_name = 'pcloud24_1'  # nome del bucket su Google Cloud Storage
    collection_prefix = 'dati' # prefisso per i dati della raccolta (ES: dati_Carla)

    process_csv_files(local_directory, bucket_name, collection_prefix)


    app.run(host='0.0.0.0', port=80, debug=True)

