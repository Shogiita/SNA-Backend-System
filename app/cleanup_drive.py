import gspread
from google.oauth2.service_account import Credentials
import json

def nuke_service_account_drive():
    print("Membuka koneksi ke Google Drive Service Account...")
    
    # Load kredensial
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file('serviceAccountKey.json', scopes=scopes)
    gc = gspread.authorize(creds)

    print("Mencari file yatim piatu (orphaned files)...")
    try:
        # Ambil SEMUA spreadsheet yang dimiliki oleh akun ini
        all_sheets = gc.openall()
        total_files = len(all_sheets)
        
        if total_files == 0:
            print("Drive sudah kosong! Tidak ada file yang perlu dihapus.")
            return

        print(f"🚨 DITEMUKAN {total_files} FILE YANG MEMENUHI KUOTA 🚨")
        print("Mulai proses penghapusan massal...")

        deleted = 0
        for sheet in all_sheets:
            print(f"Menghapus -> {sheet.title} (ID: {sheet.id})")
            gc.del_spreadsheet(sheet.id)
            deleted += 1

        print(f"\n✅ PROSES SELESAI! {deleted} file berhasil dihapus.")
        print("Kuota Google Drive Service Account Anda sekarang sudah kembali KOSONG.")

    except Exception as e:
        print(f"Terjadi kesalahan saat menghapus: {e}")

if __name__ == "__main__":
    nuke_service_account_drive()