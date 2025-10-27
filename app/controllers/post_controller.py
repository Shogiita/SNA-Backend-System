    
import asyncio
from fastapi import HTTPException
from app.database import db

# endpoint for post
async def get_all_posts_from_db():
    try:
        posts_list = []
        docs_stream = db.collection('kawanss').stream()
        docs = await asyncio.to_thread(list, docs_stream)
        
        for doc in docs:
            post_data = doc.to_dict()
            post_data['id'] = doc.id
            posts_list.append(post_data)
            
        return posts_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
