from firebase_admin import firestore
from google.cloud.firestore import FieldFilter
from datetime import datetime, timedelta

db = firestore.client()

def get_main_dashboard_summary():
    """
    Mengambil data statistik internal dari Firestore:
    - User Growth
    - Post Stats
    - Top 10 Content (FIXED: Menggunakan field 'jumlahView' dan 'judul')
    """
    try:
        now = datetime.now()
        
        users_ref = db.collection('users')
        total_users_snapshot = users_ref.count().get()
        total_users = total_users_snapshot[0][0].value

        first_day_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        new_users_this_month = len(users_ref.where(
            filter=FieldFilter('joinDate', '>=', first_day_this_month)
        ).get())
        
        new_users_last_month = len(users_ref.where(
            filter=FieldFilter('joinDate', '>=', first_day_last_month)
        ).where(
            filter=FieldFilter('joinDate', '<', first_day_this_month)
        ).get())
        
        user_growth_percent = 0
        if new_users_last_month > 0:
            user_growth_percent = (new_users_this_month / new_users_last_month) * 100
        else:
            user_growth_percent = 100.0 if new_users_this_month > 0 else 0.0

        posts_ref = db.collection('infoss')
        
        total_posts = posts_ref.count().get()[0][0].value
        
        thirty_days_ago = now - timedelta(days=30)
        new_posts_30d = len(posts_ref.where(
            filter=FieldFilter('uploadDate', '>=', thirty_days_ago)
        ).get())

        top_posts_query = posts_ref.order_by('jumlahView', direction=firestore.Query.DESCENDING).limit(10).stream()
        
        top_posts = []
        for doc in top_posts_query:
            data = doc.to_dict()
            
            top_posts.append({
                "id": doc.id,
                "judul": data.get('judul', data.get('title', 'No Title')),
                "jumlahView": data.get('jumlahView', 0),
                "kategori": data.get('kategori', 'Umum'),
                "gambar": data.get('gambar', ''),
                "uploadDate": data.get('uploadDate').isoformat() if data.get('uploadDate') else None,
                "jumlahComment": data.get('jumlahComment', 0),
                "jumlahLike": data.get('jumlahLike', 0)
            })

        integrations = {
            "google_sheets": {
                "status": "connected",
                "last_sync": datetime.now().isoformat()
            },
            "google_analytics": {
                "status": "connected",
                "active_users_now": 0
            }
        }

        return {
            "status": "success",
            "data": {
                "users": {
                    "total": total_users,
                    "total_post": total_posts,
                    "new_this_month": new_users_this_month,
                    "new_last_month": new_users_last_month,
                    "growth_percentage": round(user_growth_percent, 2),
                },
                "posts": {
                    "total": total_posts,
                    "new_30_days": new_posts_30d
                },
                "top_content": top_posts,
                "integrations": integrations
            }
        }
    except Exception as e:
        print(f"Error fetching dashboard: {e}")
        return {
            "status": "error", 
            "message": str(e),
            "data": {
                 "users": {"total": 0, "new_this_month": 0, "growth_percentage": 0},
                 "posts": {"total": 0, "new_30_days": 0},
                 "top_content": [],
                 "integrations": {}
            }
        }