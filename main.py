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
    def __init__(self, username):
        super().__init__()
        self.id = username
        self.username = username


# inizializzazione app flask
app = Flask(__name__)
app.config['SECRET_KEY'] = secret_key  # secret_key è una stringa casuale

# configurazione di flask-login (gestisce autenticazione utenti)
login = LoginManager(app)
login.login_view = '/static/login.html'  # pagina mostrata a utente non autenticato quando tenta di accedere


db = 'livelyageing'
#creo client per accedere a database firestore
db = firestore.Client.from_service_account_json('credentials.json', database=db)
#client per accedere a cloud storage
storage_client = storage.Client.from_service_account_json('credentials.json')

# database admin
admindb ={
    'marta':'gabbi'
}

# database utenti
usersdb = {
    'Carla':'carla',
    'Francesco':'francesco',
    'Luigi': 'luigi',
    'Lalla':'lalla',
    'Luciano':'luciano'
}


@app.route('/', methods=['GET', 'POST'])
def root():
    return redirect(url_for('static', filename='index.html'))


@login.user_loader  # verifica e recupera un utente a partire da username
def load_user(username):
    if username in usersdb:
        return User(username)
    return None


@app.route('/login', methods=['GET', 'POST'])
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


# converte un orario nel formato stringa in secondi
def convertToSeconds(time_str):
    hours, minutes, seconds = map(int, time_str.split(':'))  # splitta per : e converte in intero
    return hours * 3600 + minutes * 60 + seconds


# funzione per calcolare distanza media e tempo medio degli utenti
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
def grafico_admin(username):
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
def upload_to_cloud_storage(file_path, bucket_name):  # carica file locale su un bucket di Cloud Storage
    try:
        bucket = storage_client.bucket(bucket_name)
        blob_name = os.path.basename(file_path)
        blob = bucket.blob(blob_name)  # blob è un oggetto di archiviazione

        logger.info(f"Uploading file: {file_path}")

        with open(file_path, 'rb') as file:
            blob.upload_from_file(file)  # carica file locale nel blob

        logger.info(f"File {file_path} uploaded to {bucket_name}/{blob_name}.")
        return blob
    except Exception as e:
        logger.error(f"Error uploading file {file_path}: {str(e)}")
        return None


def store_in_firestore(blob, collection_name):  # scarica blob da Storage, lo elabora come CSV e inserisce i dati su Firestore
    try:
        # crea file temporaneo
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_filename = temp_file.name

        blob.download_to_filename(temp_filename)  # scarica il contenuto del blob nel file temporaneo

        df = pd.read_csv(temp_filename, sep=';', encoding='utf-8')  # leggi il CSV dal file temporaneo

        expected_columns = ['X', 'Y', 'Z', 'Tempo']
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(
                f"Le colonne del CSV non corrispondono. Attese: {expected_columns}, Trovate: {df.columns.tolist()}")

        records = df.to_dict('records')

        batch = db.batch()  #oggetto batch raccoglie op. di scrittura che saranno eseguite insieme
        for record in records:  # ogni record è una riga del CSV
            doc_id = f"{collection_name}_{record['Tempo'].replace(':', '-')}"
            doc_ref = db.collection(collection_name).document(doc_id)
            batch.set(doc_ref, record)  # scrive i dati racchiusi da record nel file identificato da doc_ref
        batch.commit()

        logger.info(f"Data from {blob.name} stored in Firestore collection {collection_name}")
    except Exception as e:
        logger.error(f"Error processing blob {blob.name}: {str(e)}")
        raise
    finally:
        # rimuovi il file temporaneo
        if 'temp_filename' in locals():
            os.unlink(temp_filename)


# processa file CSV della directory locale, li carica su Storage e memorizza su Firestore
def process_csv_files(local_directory, bucket_name, collection_prefix):
    logger.info(f"Processing CSV files from directory: {local_directory}")

    if not os.path.exists(local_directory):  # controlla che directory locale esista
        logger.error(f"Directory not found: {local_directory}")
        return

    for filename in os.listdir(local_directory):  # controlla tutti i file della directory
        if filename.endswith('.csv'):  # se fle è CSV, viene processato
            file_path = os.path.join(local_directory, filename)
            logger.info(f"Processing file: {file_path}")

            # viene processato = caricato su Storage e memorizzato su Firestore
            try:
                blob = upload_to_cloud_storage(file_path, bucket_name)
                if blob:
                    collection_name = f"{collection_prefix}_{os.path.splitext(filename)[0]}"
                    store_in_firestore(blob, collection_name)
                else:
                    logger.error(f"Failed to upload file: {file_path}")
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                # continua con il prossimo file invece d'interrompere l'intero processo


def initialize_data():  # imposta i parametri e avvia elaborazione dei CSV nella local directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    local_directory = os.path.join(base_dir, 'Dati')
    bucket_name = 'pcloud24_1'
    collection_prefix = 'dati'

    logger.info(f"Base directory: {base_dir}")
    logger.info(f"Local directory for CSV files: {local_directory}")

    process_csv_files(local_directory, bucket_name, collection_prefix)


# inizializza i dati all'avvio dell'applicazione
initialize_data()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)