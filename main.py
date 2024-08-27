import os
import pandas as pd
from datetime import datetime

from flask import Flask, request, redirect, url_for
from flask_login import LoginManager, current_user, login_user, logout_user, login_required, UserMixin
import json
from secret import secret_key
from google.cloud import firestore
from google.cloud import storage



db = 'livelyageing'
coll = 'utenti'

#creo client per accedere a database firestore
db = firestore.Client.from_service_account_json('credentials.json', database=db)
#client per accedere a cloud storage
storage_client = storage.Client.from_service_account_json('credentials.json')


class User(UserMixin): #classe utente che rappresenta gli utenti del sistema
    def __init__(self, username):
        super().__init__()
        self.id = username
        self.username = username


app = Flask(__name__)
app.config['SECRET_KEY'] = secret_key
login = LoginManager(app)
login.login_view = '/static/login.html'


usersdb = {
    'marta':'gabbi'
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
            return redirect(url_for('grafico'))

        username = request.values['u']
        password = request.values['p']

        if username in usersdb and password == usersdb[username]:
            login_user(User(username), remember=True)
            return redirect(url_for('grafico'))
        print("login fallito")

    return redirect('/static/login.html')


@app.route('/logout', methods=["POST"])
@login_required
def logout():
    logout_user()
    return redirect('/') #url_for('static', filename='index.html'


@app.route('/grafico', methods=['GET'])
@login_required
def grafico():
    '''
    #prendi i dati dal file giusto
    user_id = request.args.get('user_id') #????
    collection_ref = db.collection(user_id)
    docs = collection_ref.stream()
    dati = [doc.to_dict() for doc in docs]

    return redirect(url_for('static', filename='grafico.html')), jsonify(dati)
    '''
    return "ciao grafico"

'''
def prova_dati_su_gcloud():
    #directory_path = 'Dati'
    #bucket_name = 'pcloud24_1'

    #accedo al cloud storage
    storage_client = storage.Client.from_service_account_json('credentials.json')
    bucket = storage_client.bucket('pcloud24_1')

    for filename in os.listdir('Dati'):
        if os.path.isfile(os.path.join('Dati', filename)): #controlla che sia un file e non una cartella
            # Genera un timestamp per ogni file
            now = datetime.now()
            current_time = now.strftime('%Y_%m_%d_%H_%M_%S')
            blob_name = f'{filename}-{current_time}'

            #carica file su gcloud
            blob = bucket.blob(blob_name)
            file_path = os.path.join('Dati', filename)
            blob.upload_from_filename(file_path)
            print("caricato")

            #save_file_to_firestore(blob, filename, current_time)


def save_file_to_firestore(blob, filename, current_time):
    db = 'livelyageing'
    utenti = ['carla']

    db = firestore.Client.from_service_account_json('credentials.json', database=db)

    file_data = blob.download_as_text()
    
    for u in filename:
        doc_ref = db.collection('utenti').document(f'{filename}')
        
        
        prova_ref = doc_ref.collection('posizioni').document(())
        doc_ref.set({
            'filename': filename,
            'content': file_data,
            'timestamp': current_time
        })
'''

def save_file_to_firestore(blob, filename):
    # Connetti al database Firestore
    db = firestore.Client.from_service_account_json('credentials.json')

    # Scarica il contenuto del file come testo
    file_data = blob.download_as_text()

    # Crea un riferimento al documento nella collezione 'utenti' con il nome del file
    doc_ref = db.collection('utenti').document(filename)

    # Salva i dati nel documento Firestore
    doc_ref.set({
        'filename': filename,
        'content': file_data
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)

    #prova_dati_su_gcloud()

    blob = 'Carla.csv'
    filename = 'carla'
    save_file_to_firestore()