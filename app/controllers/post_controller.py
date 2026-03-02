import asyncio
from fastapi import HTTPException
from app.database import db

async def get_all_posts_from_db(limit: int = None):
    """Logika untuk mengambil post dari koleksi 'kawanss'."""
    try:
        posts_list = []
        query = db.collection('kawanss')
        
        if limit:
            query = query.limit(limit)
            
        docs_stream = query.stream()
        docs = await asyncio.to_thread(list, docs_stream)
        
        for doc in docs:
            post_data = doc.to_dict()
            if post_data:
                post_data['id'] = doc.id
                posts_list.append(post_data)
            
        return posts_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal mengambil data posts (kawanss): {str(e)}")