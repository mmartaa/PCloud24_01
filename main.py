import os
import pandas as pd
import tempfile


from flask import Flask, request, redirect, url_for, jsonify
from flask_login import LoginManager, current_user, login_user, logout_user, login_required, UserMixin
from secret import secret_key
from google.cloud import firestore
from google.cloud import storage


class User(UserMixin): #classe utente che rappresenta gli utenti del sistema
    def __init__(self, username, is_admin=False):
        super().__init__()
        self.id = username
        self.username = username



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
    'Luigi': 'luigi'
}



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

        # controlla se username e password sono in usersdb
        if username in usersdb and password == usersdb[username]:
            login_user(User(username), remember=True)
            return redirect(url_for('data'))
        print("login utente fallito")

        # controlla se username e password sono in admindb
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


def convertToSeconds(time_str):
    hours, minutes, seconds = map(int, time_str.split(':'))
    return hours * 3600 + minutes * 60 + seconds


def media_distanza_tempo(usersdb):
    totale_tempo = 0
    totale_distanza = 0
    numero_utenti = len(usersdb)

    for username in usersdb.keys():
        collection_ref = db.collection(f'dati_{username}')
        docs = collection_ref.stream()

        distanza_utente = 0
        punti = []
        tempi = []
        for doc in docs:
            doc_data = doc.to_dict()
            punti.append((doc_data['X'], doc_data['Z']))
            tempi.append(doc_data['Tempo'])

        # calcolo distanza
        for i in range(1, len(punti)):
            delta_x = punti[i][0] - punti[i - 1][0]
            delta_z = punti[i][1] - punti[i - 1][1]
            distanza_utente += (delta_x**2 + delta_z**2) ** 0.5

        totale_distanza += distanza_utente
        print(totale_distanza)

        # calcolo tempo
        if len(tempi) > 0:
            tempoInizio = convertToSeconds(tempi[0])
            tempoFine = convertToSeconds(tempi[-1])
            tempo_delta = tempoFine - tempoInizio
            totale_tempo += tempo_delta

    # calcolo distanza e tempo medi
    if numero_utenti > 0:
        distanza_media = totale_distanza / numero_utenti
        tempo_medio = totale_tempo / numero_utenti

        minuti_medi = int(tempo_medio // 60)
        secondi_medi = int(tempo_medio % 60)
    else:
        distanza_media = 0
        minuti_medi = 0
        secondi_medi = 0

    return distanza_media, minuti_medi, secondi_medi


@app.route('/utenti/<username>', methods=['GET'])
def user_graph(username):
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

    distanza_media, minuti_medi, secondi_medi = media_distanza_tempo(usersdb)

    return jsonify({
        'utente_data': data,
        'distanza_media': distanza_media,
        'tempo_medio': {
            'minuti': minuti_medi,
            'secondi': secondi_medi
        }
    })


@app.route('/grafico', methods=['GET']) # grafico per il singolo utente
@login_required
def grafico():
    username = current_user.username
    print(current_user.username)

    collection_ref = db.collection(f'dati_%s'%username) # il % fa da segnaposto
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


@app.route('/data', methods=['GET'])
@login_required
def data():
    return redirect(url_for('static', filename='grafico.html'))



# CARICA FILE SU CLOUD STORAGE E MEMORIZZA SU FIRESTORE
'''
def upload_to_cloud_storage(file_path, bucket_name):
    # Carica file su Google Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"File {file_path} uploaded to {bucket_name}/{blob_name}.")
    return blob

def store_in_firestore(blob, collection_name):
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
            store_in_firestore(blob, collection_name)


if __name__ == '__main__':
    # carica i dati prima di far partire app Flask
    local_directory = 'Dati'  # local directory con i file csv
    bucket_name = 'pcloud24_1'  # nome del bucket su Google Cloud Storage
    collection_prefix = 'dati' # prefisso per i dati della raccolta (ES: dati_Carla)

    process_csv_files(local_directory, bucket_name, collection_prefix)


    app.run(host='0.0.0.0', port=80, debug=True)
'''


def upload_to_cloud_storage(file_path, bucket_name):
    bucket = storage_client.bucket(bucket_name)
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)

    with open(file_path, 'rb') as file:
        blob.upload_from_file(file)

    print(f"File {file_path} uploaded to {bucket_name}/{blob_name}.")
    return blob


def store_in_firestore(blob, collection_name):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        temp_filename = temp_file.name

    try:
        blob.download_to_filename(temp_filename)
        df = pd.read_csv(temp_filename, sep=';')
        records = df.to_dict('records')

        for record in records:
            doc_id = f"{collection_name}_{str(record['Tempo']).replace(' ', '_').replace(':', '-')}"
            doc_ref = db.collection(collection_name).document(doc_id)
            doc_ref.set(record)

        print(f"Data from {blob.name} stored in Firestore collection {collection_name}")

    finally:
        os.unlink(temp_filename)


def process_csv_files(local_directory, bucket_name, collection_prefix):
    for filename in os.listdir(local_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(local_directory, filename)
            blob = upload_to_cloud_storage(file_path, bucket_name)
            collection_name = f"{collection_prefix}_{os.path.splitext(filename)[0]}"
            store_in_firestore(blob, collection_name)


if __name__ == '__main__':
    local_directory = os.path.join(os.path.dirname(__file__), 'Dati')
    bucket_name = 'pcloud24_1'
    collection_prefix = 'dati'

    # Check if running on GCP (App Engine)
    if os.getenv('GAE_ENV', '').startswith('standard'):
        # If on GCP, assume files are already in Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix='Dati/')
        for blob in blobs:
            if blob.name.endswith('.csv'):
                collection_name = f"{collection_prefix}_{os.path.splitext(os.path.basename(blob.name))[0]}"
                store_in_firestore(blob, collection_name)
    else:
        # If running locally, process files from local directory
        process_csv_files(local_directory, bucket_name, collection_prefix)

    app.run(host='0.0.0.0', port=8080, debug=True)
