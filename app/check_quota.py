import requests
import google.auth.transport.requests
from google.oauth2.service_account import Credentials

def check_service_account_drive():
    print("Mendapatkan otorisasi Service Account...")
    scopes = ['https://www.googleapis.com/auth/drive']
    
    # Pastikan nama file JSON kredensial Anda sesuai
    creds = Credentials.from_service_account_file('serviceAccountKey.json', scopes=scopes)
    
    request = google.auth.transport.requests.Request()
    creds.refresh(request)
    headers = {"Authorization": f"Bearer {creds.token}"}
    
    print("\n--- INFORMASI KUOTA STORAGE ---")
    # Endpoint untuk mengecek kuota
    about_url = "https://www.googleapis.com/drive/v3/about?fields=storageQuota"
    resp_about = requests.get(about_url, headers=headers)
    
    if resp_about.status_code == 200:
        quota = resp_about.json().get('storageQuota', {})
        limit_gb = int(quota.get('limit', 0)) / (1024**3) if quota.get('limit') else 15.0
        usage_gb = int(quota.get('usage', 0)) / (1024**3)
        usage_trash_gb = int(quota.get('usageInDriveTrash', 0)) / (1024**3)
        
        print(f"Kapasitas Maksimal : {limit_gb:.2f} GB")
        print(f"Total Terpakai     : {usage_gb:.2f} GB")
        print(f"Terpakai di Trash  : {usage_trash_gb:.2f} GB")
        print(f"Sisa Kuota         : {(limit_gb - usage_gb):.2f} GB")
    else:
        print("Gagal mengecek kuota:", resp_about.text)

    print("\n--- 10 FILE TERBESAR DI SERVICE ACCOUNT ---")
    # Endpoint untuk melihat daftar file, diurutkan dari ukuran terbesar
    files_url = "https://www.googleapis.com/drive/v3/files?orderBy=quotaBytesUsed desc&pageSize=10&fields=files(name,size,mimeType,trashed)"
    resp_files = requests.get(files_url, headers=headers)
    
    if resp_files.status_code == 200:
        files = resp_files.json().get('files', [])
        if not files:
            print("Drive Service Account ini kosong.")
        else:
            for i, f in enumerate(files, 1):
                name = f.get('name', 'Unknown')
                size_mb = int(f.get('size', 0)) / (1024**2)
                is_trashed = "🗑️ (Di tempat sampah)" if f.get('trashed') else "✅ (Aktif)"
                print(f"{i}. {name} | {size_mb:.2f} MB | {is_trashed}")
    else:
        print("Gagal mengambil daftar file:", resp_files.text)

if __name__ == "__main__":
    check_service_account_drive()