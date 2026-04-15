import pytest
from unittest.mock import patch, MagicMock, mock_open

# =====================================================================
# 1. TEST DASHBOARD UTAMA
# =====================================================================
@patch("app.controllers.report_controller.neo4j_driver.session")
@patch("app.controllers.report_controller.get_realtime_active_users")
def test_dashboard_summary_success(mock_ga_users, mock_session, api_client):
    """
    Menguji Endpoint utama dashboard yang menggabungkan stat, top content, dan SNA.
    Fungsi ini bersifat synchronous di endpointnya, jadi tidak menggunakan mark.asyncio
    """
    # Arrange: Mock response dari Google Analytics
    mock_ga_users.return_value = {"last_30_min": 15, "last_5_min": 5}

    # Arrange: Mock Neo4j Session & Transaction
    mock_tx = MagicMock()
    
    # a. Mock data untuk Query 1 (Statistik Global)
    mock_stat_res = MagicMock()
    mock_stat_res.single.return_value = {
        "total_users": 100, "total_infoss": 50, "total_kawanss": 200,
        "new_users_this_month": 10, "new_users_last_month": 5,
        "new_infoss_30_days": 15, "new_kawanss_30_days": 30
    }
    
    # b. Mock data untuk Query 2 (Top Content)
    mock_top_content_res = MagicMock()
    mock_top_content_res.data.return_value = [
        {"id": "p1", "judul": "Berita 1", "jumlahView": 100, "kategori": "Umum", "gambar": "", "uploadDate": "", "jumlahComment": 0, "jumlahLike": 0}
    ]

    # c. Mock data untuk Query 3 (SNA Records untuk kalkulasi graf)
    mock_sna_res = MagicMock()
    mock_sna_res.data.return_value = [
        {"uid": "user_1", "uname": "Jonathan", "username": "jonathan123", "pid": "p1"},
        {"uid": "user_2", "uname": "Budi", "username": "budi123", "pid": "p1"}
    ]
    
    # Atur agar pemanggilan session.run secara berurutan mengembalikan mock yang sesuai
    mock_tx.run.side_effect = [mock_stat_res, mock_top_content_res, mock_sna_res]
    mock_session.return_value.__enter__.return_value = mock_tx

    # Act
    response = api_client.get("/report/dashboard")

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Validasi Statistik Terhitung Benar
    assert data["data"]["users"]["total"] == 100
    assert data["data"]["users"]["growth_percentage"] == 200.0 # (10 / 5) * 100
    
    # Validasi Data Top Content Terbaca
    assert len(data["data"]["top_content"]) == 1
    assert data["data"]["top_content"][0]["judul"] == "Berita 1"
    
    # Validasi SNA Terhitung Berdasarkan Mock
    assert "top_10_centrality" in data["data"]
    assert data["data"]["integrations"]["google_analytics"]["active_users_last_30_min"] == 15


# =====================================================================
# 2. TEST GOOGLE ANALYTICS (REALTIME)
# =====================================================================
@patch("app.controllers.report_controller.config")
@patch("app.controllers.report_controller.BetaAnalyticsDataClient")
def test_get_realtime_active_users_ga_error(mock_ga_client, mock_config):
    """
    Menguji bahwa jika API Google Analytics down/menolak request, 
    Sistem tidak akan crash, melainkan mengembalikan nilai fallback (200).
    """
    from app.controllers.report_controller import get_realtime_active_users
    
    # Arrange: Beri nilai dummy pada GA_PROPERTY_ID agar lolos pengecekan IF
    mock_config.GA_PROPERTY_ID = "12345678"
    mock_config.FIREBASE_CREDENTIALS = {"private_key": "dummy_key"}
    
    # Simulasi Google Analytics API Error
    mock_ga_client.side_effect = Exception("GA API Error: Quota Exceeded")
    
    # Act
    result = get_realtime_active_users()
    
    # Assert
    assert result["last_30_min"] == 200
    assert result["last_5_min"] == 200


# =====================================================================
# 3. TEST TOP HASHTAGS
# =====================================================================
@patch("app.controllers.report_controller.neo4j_driver.session")
def test_get_top_10_hashtags_success(mock_session, api_client):
    """ Menguji logika Regex ekstraksi hashtag dari post di Neo4j """
    mock_tx = MagicMock()
    mock_tx.run.return_value.data.return_value = [
        {"text": "Halo warga #Surabaya! Mari jaga kebersihan."},
        {"text": "Info lalu lintas di #Surabaya sangat padat hari ini."},
        {"text": "Keren sekali acara #JatimFest2026"}
    ]
    mock_session.return_value.__enter__.return_value = mock_tx
    
    response = api_client.get("/report/top-hashtags")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "success"
    
    # Surabaya harusnya muncul 2 kali, JatimFest2026 muncul 1 kali
    top_tags = data["data"]
    assert top_tags[0]["hashtag"] == "#surabaya"
    assert top_tags[0]["count"] == 2
    assert top_tags[1]["hashtag"] == "#jatimfest2026"


# =====================================================================
# 4. TEST EXPORT REPORT (NEO4J & INSTAGRAM)
# =====================================================================
@patch("app.controllers.report_controller.neo4j_driver.session")
def test_export_neo4j_csv_success(mock_session, api_client):
    """ Menguji Export CSV dari sumber data Neo4j """
    mock_tx = MagicMock()
    mock_tx.run.return_value.data.return_value = [
        {"ID": "u001", "Nama": "Esther Irawati", "Total_Post": 15},
        {"ID": "u002", "Nama": "Jonathan", "Total_Post": 10}
    ]
    mock_session.return_value.__enter__.return_value = mock_tx
    
    response = api_client.get("/report/export/csv/neo4j")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "attachment; filename=Laporan_SNA_Neo4j.csv" in response.headers["content-disposition"]
    
    # Pastikan data ada di dalam string CSV yang dikembalikan
    assert "Esther Irawati" in response.text
    assert "15" in response.text

@patch("app.controllers.report_controller.neo4j_driver.session")
def test_export_neo4j_csv_empty(mock_session, api_client):
    """ Menguji penolakan export jika Neo4j kosong """
    mock_tx = MagicMock()
    mock_tx.run.return_value.data.return_value = [] # DB Kosong
    mock_session.return_value.__enter__.return_value = mock_tx
    
    response = api_client.get("/report/export/csv/neo4j")
    assert response.status_code == 404
    assert "Tidak ada data" in response.json()["detail"]


@patch("os.path.exists", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data='[{"id": "ig_1", "timestamp": "2026-04-09", "like_count": 150, "interactions": [{"id": 1}], "caption": "SNA is cool!"}]')
def test_export_instagram_csv_success(mock_file, mock_exists, api_client):
    """ Menguji Export CSV dengan membaca data cache JSON Instagram """
    response = api_client.get("/report/export/csv/instagram")
    
    assert response.status_code == 200
    assert "attachment; filename=Laporan_SNA_Instagram.csv" in response.headers["content-disposition"]
    assert "SNA is cool!" in response.text
    assert "ig_1" in response.text

@patch("os.path.exists", return_value=False)
def test_export_instagram_csv_no_cache(mock_exists, api_client):
    """ Menguji gagal export IG jika belum di-ingest (Cache file tidak ada) """
    response = api_client.get("/report/export/csv/instagram")
    assert response.status_code == 404
    assert "Cache Instagram tidak ditemukan" in response.json()["detail"]


# =====================================================================
# 5. TEST STATIC ANALYTICS SUMMARY
# =====================================================================
def test_get_analytics_summary(api_client):
    response = api_client.get("/report/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "active_users" in data["data"]