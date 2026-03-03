import firebase_admin
from firebase_admin import credentials, firestore
from neo4j import GraphDatabase
from app import config

if not config.FIREBASE_CREDENTIALS["private_key"]:
    raise ValueError("Firebase Private Key tidak ditemukan di environment variables!")

cred = credentials.Certificate(config.FIREBASE_CREDENTIALS)

try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(cred)

db = firestore.client()

neo4j_driver = GraphDatabase.driver(
    config.NEO4J_URI, 
    auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
)

def get_neo4j_session():
    return neo4j_driver.session()