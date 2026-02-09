import firebase_admin
from firebase_admin import credentials, firestore
from app import config
if not config.FIREBASE_CREDENTIALS["private_key"]:
    raise ValueError("Firebase Private Key tidak ditemukan di environment variables!")

cred = credentials.Certificate(config.FIREBASE_CREDENTIALS)

try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(cred)

db = firestore.client()