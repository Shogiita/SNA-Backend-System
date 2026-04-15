import pytest
import pandas as pd
from unittest.mock import patch
from app.controllers.ml_controller import preprocess_text

# TEST 1: Pure Logic Function (Tidak butuh Mock)
def test_preprocess_text_cleans_properly():
    raw_text = "Halo!! Ini Tweet #SNA @SBY_123"
    expected = "halo ini tweet sna sby"
    
    assert preprocess_text(raw_text) == expected

def test_preprocess_text_handles_empty():
    assert preprocess_text(None) == "none"
    assert preprocess_text("") == ""

# TEST 2: Endpoint testing dengan Mocking Pandas
@patch("app.controllers.ml_controller.pd.read_csv")
def test_train_model_endpoint(mock_read_csv, api_client):
    # Arrange: Buat Dummy DataFrame Kecil
    dummy_df = pd.DataFrame({
        "Username": ["A", "B", "C", "D"],
        "Tweet_ID": [1, 2, 3, 4],
        "Text": ["Bagus banget!", "Buruk jelek", "Keren", "Biasa saja"],
        "Likes": [100, 10, 80, 5] 
    })
    mock_read_csv.return_value = dummy_df

    # Act
    response = api_client.get("/ml/train")

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Model ML berhasil dilatih"
    assert "model_accuracy_on_test_data" in data