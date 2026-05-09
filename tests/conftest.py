import os
import sys
import types
from unittest.mock import MagicMock

import pytest


# ============================================================
# MOCK ENVIRONMENT VARIABLES BEFORE APP IMPORT
# ============================================================

os.environ.setdefault("FB_APP_ID", "test_fb_app_id")
os.environ.setdefault("INSTAGRAM_APP_ID", "test_instagram_app_id")
os.environ.setdefault("INSTAGRAM_APP_SECRET", "test_instagram_app_secret")
os.environ.setdefault("INSTAGRAM_BUSINESS_ACCOUNT_ID", "test_business_account_id")
os.environ.setdefault("INSTAGRAM_USERNAME", "test_instagram_username")
os.environ.setdefault("BUSINESS_ID", "test_business_id")
os.environ.setdefault("INSTAGRAM_ACCESS_TOKEN", "test_instagram_access_token")

os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
os.environ.setdefault("NEO4J_USER", "neo4j")
os.environ.setdefault("NEO4J_PASSWORD", "test_neo4j_password")
os.environ.setdefault("NEO4J_API_URL", "http://localhost:7474")
os.environ.setdefault("NEO4J_ID", "test_neo4j_id")

os.environ.setdefault("FIREBASE_TYPE", "service_account")
os.environ.setdefault("FIREBASE_PROJECT_ID", "test-project")
os.environ.setdefault("FIREBASE_PRIVATE_KEY_ID", "test_private_key_id")
os.environ.setdefault(
    "FIREBASE_PRIVATE_KEY",
    "-----BEGIN PRIVATE KEY-----\\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCtest\\n"
    "-----END PRIVATE KEY-----\\n",
)
os.environ.setdefault("FIREBASE_CLIENT_EMAIL", "firebase-adminsdk-test@test-project.iam.gserviceaccount.com")
os.environ.setdefault("FIREBASE_CLIENT_ID", "test_client_id")
os.environ.setdefault("FIREBASE_AUTH_URI", "https://accounts.google.com/o/oauth2/auth")
os.environ.setdefault("FIREBASE_TOKEN_URI", "https://oauth2.googleapis.com/token")
os.environ.setdefault("FIREBASE_AUTH_PROVIDER_CERT_URL", "https://www.googleapis.com/oauth2/v1/certs")
os.environ.setdefault("FIREBASE_CLIENT_CERT_URL", "https://www.googleapis.com/robot/v1/metadata/x509/test")

os.environ.setdefault("GA_PROPERTY_ID", "test_ga_property_id")

os.environ.setdefault("GCP_TYPE", "service_account")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("GCP_PRIVATE_KEY_ID", "test_gcp_private_key_id")
os.environ.setdefault(
    "GCP_PRIVATE_KEY",
    "-----BEGIN PRIVATE KEY-----\\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCtest\\n"
    "-----END PRIVATE KEY-----\\n",
)
os.environ.setdefault("GCP_CLIENT_EMAIL", "test@test-project.iam.gserviceaccount.com")
os.environ.setdefault("GCP_CLIENT_ID", "test_gcp_client_id")
os.environ.setdefault("GCP_AUTH_URI", "https://accounts.google.com/o/oauth2/auth")
os.environ.setdefault("GCP_TOKEN_URI", "https://oauth2.googleapis.com/token")
os.environ.setdefault("GCP_AUTH_PROVIDER_CERT_URL", "https://www.googleapis.com/oauth2/v1/certs")
os.environ.setdefault("GCP_CLIENT_CERT_URL", "https://www.googleapis.com/robot/v1/metadata/x509/test")


# ============================================================
# MOCK app.database BEFORE REAL app.database IS IMPORTED
# ============================================================

fake_database_module = types.ModuleType("app.database")
fake_database_module.db = MagicMock(name="mock_firestore_db")
fake_database_module.neo4j_driver = MagicMock(name="mock_neo4j_driver")

sys.modules["app.database"] = fake_database_module


# ============================================================
# MOCK firebase_admin BEFORE APP IMPORT
# ============================================================

fake_firebase_admin = types.ModuleType("firebase_admin")
fake_firebase_auth = types.ModuleType("firebase_admin.auth")
fake_firebase_credentials = types.ModuleType("firebase_admin.credentials")
fake_firebase_firestore = types.ModuleType("firebase_admin.firestore")

fake_firebase_admin._apps = {"test": object()}
fake_firebase_admin.initialize_app = MagicMock(name="initialize_app")

fake_firebase_auth.verify_id_token = MagicMock(
    return_value={
        "uid": "admin-test-uid",
        "email": "admin@test.com",
        "name": "Test Admin",
        "admin": True,
    }
)

fake_firebase_credentials.Certificate = MagicMock(name="Certificate")
fake_firebase_firestore.client = MagicMock(name="firestore_client")

fake_firebase_admin.auth = fake_firebase_auth
fake_firebase_admin.credentials = fake_firebase_credentials
fake_firebase_admin.firestore = fake_firebase_firestore

sys.modules["firebase_admin"] = fake_firebase_admin
sys.modules["firebase_admin.auth"] = fake_firebase_auth
sys.modules["firebase_admin.credentials"] = fake_firebase_credentials
sys.modules["firebase_admin.firestore"] = fake_firebase_firestore


# ============================================================
# FIXTURE
# ============================================================

@pytest.fixture(scope="function")
def mock_db():
    fake_database_module.db.reset_mock()
    return fake_database_module.db


@pytest.fixture(scope="function")
def mock_neo4j_driver():
    fake_database_module.neo4j_driver.reset_mock()
    return fake_database_module.neo4j_driver


@pytest.fixture(scope="function")
def mock_admin():
    return {
        "uid": "admin-test-uid",
        "email": "admin@test.com",
        "name": "Test Admin",
        "claims": {"admin": True},
    }


@pytest.fixture(scope="function")
def api_client(mock_admin):
    from fastapi.testclient import TestClient
    from app.main import app
    from app.middleware.firebase_auth import get_current_admin

    app.dependency_overrides[get_current_admin] = lambda: mock_admin

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()