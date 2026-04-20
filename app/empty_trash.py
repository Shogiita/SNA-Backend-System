import requests
import google.auth.transport.requests
from google.oauth2.service_account import Credentials

def empty_trash():
    print("Mendapatkan otorisasi Service Account...")
    scopes = ['https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('serviceAccountKey.json', scopes=scopes)
    
    # Refresh/Minta token API
    request = google.auth.transport.requests.Request()
    creds.refresh(request)
    
    print("Menghapus permanen SEMUA file yang ada di Trash...")
    headers = {
        "Authorization": f"Bearer {creds.token}"
    }
    
    # Menembak Endpoint Google Drive v3 untuk "Empty Trash"
    response = requests.delete("https://www.googleapis.com/drive/v3/files/trash", headers=headers)
    
    if response.status_code in [200, 204]:
        print("✅ BERHASIL! Trash (Tempat Sampah) berhasil dikosongkan.")
        print("✅ Kuota penyimpanan Google Drive Anda kini telah kembali 100% Free.")
    else:
        print(f"❌ Terjadi kesalahan: {response.status_code} - {response.text}")

if __name__ == "__main__":
    empty_trash()