from fastapi import HTTPException
from app.database import db 

def get_test_message():
    """Logika untuk endpoint tes."""
    return {"status": "success"}

def create_new_user(user_data: dict):
    """Logika untuk membuat pengguna baru."""
    try:
        doc_ref = db.collection('users').add(user_data)
        return {"message": "Pengguna berhasil dibuat", "user_id": doc_ref[1].id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def get_user_by_id(user_id: str):
    """Logika untuk mengambil satu pengguna berdasarkan ID."""
    try:
        doc = db.collection('users').document(user_id).get()
        if doc.exists:
            return doc.to_dict()
        else:
            raise HTTPException(status_code=404, detail="Pengguna tidak ditemukan")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def get_all_users_from_db():
    """Logika untuk mengambil semua pengguna."""
    try:
        users_list = []
        docs = db.collection('users').stream()
        for doc in docs:
            user_data = doc.to_dict()
            user_data['id'] = doc.id
            users_list.append(user_data)
        return users_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
