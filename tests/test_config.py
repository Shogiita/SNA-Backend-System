import importlib


def test_config_prints_warning_when_instagram_access_token_missing(monkeypatch, capsys):
    import app.config as config

    monkeypatch.setenv(
        "INSTAGRAM_ACCESS_TOKEN",
        "MASUKKAN_ACCESS_TOKEN_ANDA_YANG_SUDAH_DIGENERATE",
    )

    importlib.reload(config)

    captured = capsys.readouterr()

    assert "PERINGATAN: INSTAGRAM_ACCESS_TOKEN belum diatur di file .env." in captured.out

    monkeypatch.setenv("INSTAGRAM_ACCESS_TOKEN", "test_instagram_access_token")

    importlib.reload(config)