import os
import pandas as pd
import tempfile
import logging

from flask import Flask, request, redirect, url_for, jsonify
from flask_login import LoginManager, current_user, login_user, logout_user, login_required, UserMixin
from secret import secret_key
from google.cloud import firestore
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    try:
        bucket = storage_client.bucket(bucket_name)
        blob_name = os.path.basename(file_path)
        blob = bucket.blob(blob_name)

        logger.info(f"Uploading file: {file_path}")

        with open(file_path, 'rb') as file:
            blob.upload_from_file(file)

        logger.info(f"File {file_path} uploaded to {bucket_name}/{blob_name}.")
        return blob
    except Exception as e:
        logger.error(f"Error uploading file {file_path}: {str(e)}")
        return None


def store_in_firestore(blob, collection_name):
    try:
        # Crea un file temporaneo
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_filename = temp_file.name

        # Scarica il contenuto del blob nel file temporaneo
        blob.download_to_filename(temp_filename)

        # Leggi il CSV dal file temporaneo
        df = pd.read_csv(temp_filename, sep=';', encoding='utf-8')

        # Assicurati che i nomi delle colonne corrispondano
        expected_columns = ['X', 'Y', 'Z', 'Tempo']
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(
                f"Le colonne del CSV non corrispondono. Attese: {expected_columns}, Trovate: {df.columns.tolist()}")

        records = df.to_dict('records')

        # Batch write per migliorare le prestazioni
        batch = db.batch()
        for record in records:
            # Usa il timestamp come parte dell'ID del documento
            doc_id = f"{collection_name}_{record['Tempo'].replace(':', '-')}"
            doc_ref = db.collection(collection_name).document(doc_id)
            batch.set(doc_ref, record)

        # Commit del batch
        batch.commit()

        logger.info(f"Data from {blob.name} stored in Firestore collection {collection_name}")
    except Exception as e:
        logger.error(f"Error processing blob {blob.name}: {str(e)}")
        raise  # Rilancia l'eccezione per gestirla nel chiamante
    finally:
        # Rimuovi il file temporaneo
        if 'temp_filename' in locals():
            os.unlink(temp_filename)


def process_csv_files(local_directory, bucket_name, collection_prefix):
    logger.info(f"Processing CSV files from directory: {local_directory}")

    if not os.path.exists(local_directory):
        logger.error(f"Directory not found: {local_directory}")
        return

    for filename in os.listdir(local_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(local_directory, filename)
            logger.info(f"Processing file: {file_path}")

            try:
                blob = upload_to_cloud_storage(file_path, bucket_name)
                if blob:
                    collection_name = f"{collection_prefix}_{os.path.splitext(filename)[0]}"
                    store_in_firestore(blob, collection_name)
                else:
                    logger.error(f"Failed to upload file: {file_path}")
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                # Continua con il prossimo file invece di interrompere l'intero processo


def initialize_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    local_directory = os.path.join(base_dir, 'Dati')
    bucket_name = 'pcloud24_1'
    collection_prefix = 'dati'

    logger.info(f"Base directory: {base_dir}")
    logger.info(f"Local directory for CSV files: {local_directory}")

    process_csv_files(local_directory, bucket_name, collection_prefix)


# Inizializza i dati all'avvio dell'applicazione
initialize_data()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)