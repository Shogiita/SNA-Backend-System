import asyncio
from fastapi import HTTPException
from app.database import db

async def get_test_message():
    return {"status": "success"}

async def create_new_user(user_data: dict):
    try:
        doc_ref = await asyncio.to_thread(db.collection('users').add, user_data)
        return {"message": "Pengguna berhasil dibuat", "user_id": doc_ref[1].id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

async def get_user_by_id(user_id: str):
    try:
        doc_ref = db.collection('users').document(user_id)
        doc = await asyncio.to_thread(doc_ref.get)
        if doc.exists:
            return doc.to_dict()
        else:
            raise HTTPException(status_code=404, detail="Pengguna tidak ditemukan")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

async def get_all_users_from_db():
    """Logika untuk mengambil semua pengguna dari koleksi 'users'."""
    try:
        users_list = []
        docs_stream = db.collection('users').select(['nama']).stream()
        docs = await asyncio.to_thread(list, docs_stream)
        
        for doc in docs:
            user_data = doc.to_dict()
            if user_data:
                user_data['id'] = doc.id
                users_list.append(user_data)
            
        return users_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal mengambil data users: {str(e)}")