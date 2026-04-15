import os
import pytest
from unittest.mock import patch, MagicMock
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="session", autouse=True)
def mock_env_vars():
    with patch.dict(os.environ, {
        "FB_APP_ID": os.getenv("FB_APP_ID", ""),
        "INSTAGRAM_APP_ID": os.getenv("INSTAGRAM_APP_ID", ""),
        "INSTAGRAM_APP_SECRET": os.getenv("INSTAGRAM_APP_SECRET", ""),
        "INSTAGRAM_BUSINESS_ACCOUNT_ID": os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID", ""),
        "INSTAGRAM_ACCESS_TOKEN": os.getenv("INSTAGRAM_ACCESS_TOKEN", "mock_token"),
        
        "NEO4J_URI": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        "NEO4J_PASSWORD": os.getenv("NEO4J_PASSWORD", ""),
        "NEO4J_USER": os.getenv("NEO4J_USER", ""),
        
        "FIREBASE_PROJECT_ID": os.getenv("FIREBASE_PROJECT_ID", "mock_project"),
        "FIREBASE_PRIVATE_KEY": os.getenv("FIREBASE_PRIVATE_KEY", ""),
        "FIREBASE_CLIENT_EMAIL": os.getenv("FIREBASE_CLIENT_EMAIL", "mock@email.com"),
        
        "GA_PROPERTY_ID": os.getenv("GA_PROPERTY_ID", "")
    }):
        yield

@pytest.fixture(scope="function", autouse=True)
def mock_databases():
    with patch("app.database.db") as mock_db, \
         patch("app.database.neo4j_driver") as mock_neo4j:
        yield mock_db, mock_neo4j

@pytest.fixture(scope="function")
def api_client():
    from fastapi.testclient import TestClient
    from app.main import app
    return TestClient(app)