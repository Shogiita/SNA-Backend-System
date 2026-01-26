from fastapi import APIRouter, HTTPException
from app.controllers.sna_graph_controller import SnaGraphController

router = APIRouter(prefix="/graph", tags=["SNA Neo4j"])
controller = SnaGraphController()

# router = APIRouter(
#     prefix="/graph",
#     tags=["Graph"]
# )

@router.get("/generate")
async def generate_graph_endpoint():
    return await graph_controller.create_social_graph()

@router.get("/generate/pajek")
async def generate_pajek_graph_endpoint():
    return await graph_controller.create_social_graph_pajek()

@router.post("/insert-test")
async def test_insert():
    # Data Dummy untuk tes koneksi
    user = {"username": "netizen_surabaya", "name": "Budi Wani", "followers_count": 500}
    post = {"id": "post_macet_waru", "caption": "Macet total di bundaran waru!"}
    user = {"username": "netizen_surabaya", "name": "Budi 3", "followers_count": 5200}
    post = {"id": "post_macet_waru", "caption": "Macet total di bundaran waru!"}
    user = {"username": "netizen_surabaya", "name": "1 Wani", "followers_count": 2500}
    post = {"id": "12421", "caption": "Macet 123 di bundaran waru!"}
    user = {"username": "netizen_surabaya", "name": "Budi 3", "followers_count": 1500}
    post = {"id": "12421", "caption": "Macet total di 1412 waru!"}
    user = {"username": "netizen_surabaya", "name": "Test 2", "followers_count": 11500}
    post = {"id": "12421", "caption": "Macet total di 1521312313 waru!"}
    
    success = controller.add_interaction(user, post, "COMMENTED")
    if success:
        return {"status": "success", "message": "Data graph berhasil disimpan ke Aura"}
    else:
        raise HTTPException(status_code=500, detail="Gagal menyimpan ke Aura")

@router.get("/top-users")
async def get_top_users():
    data = controller.get_top_active_users()
    return {"data": data}

    # Tambahkan di app/routers/graph_router.py

@router.post("/seed-dummy")
async def seed_dummy_data():
    """
    Mengisi database dengan data dummy yang kompleks untuk keperluan demo SNA.
    """
    dummy_data = [
        # --- KELOMPOK 1: KASUS MACET WARU (Viral) ---
        {
            "user": {"username": "cak_sodiq", "name": "Cak Sodiq", "followers_count": 1200},
            "post": {"id": "post_macet_waru", "caption": "Macet total di bundaran waru! Hindari jam pulang kerja."},
            "type": "COMMENTED"
        },
        {
            "user": {"username": "ning_tini", "name": "Ning Tini", "followers_count": 450},
            "post": {"id": "post_macet_waru", "caption": "Macet total di bundaran waru! Hindari jam pulang kerja."},
            "type": "LIKED"
        },
        {
            "user": {"username": "bonek_1927", "name": "Bonek Sejati", "followers_count": 5000},
            "post": {"id": "post_macet_waru", "caption": "Macet total di bundaran waru! Hindari jam pulang kerja."},
            "type": "COMMENTED"
        },
        {
            "user": {"username": "surabaya_foodie", "name": "Kuliner Suroboyo", "followers_count": 15000},
            "post": {"id": "post_macet_waru", "caption": "Macet total di bundaran waru! Hindari jam pulang kerja."},
            "type": "COMMENTED"
        },

        # --- KELOMPOK 2: KASUS KECELAKAAN MERR (Isu Lokal) ---
        {
            "user": {"username": "cak_sodiq", "name": "Cak Sodiq", "followers_count": 1200}, # User lama komen di post baru
            "post": {"id": "post_laka_merr", "caption": "Kecelakaan tunggal di MERR, harap hati-hati."},
            "type": "COMMENTED"
        },
        {
            "user": {"username": "polisi_jatim", "name": "Humas Polda Jatim", "followers_count": 25000},
            "post": {"id": "post_laka_merr", "caption": "Kecelakaan tunggal di MERR, harap hati-hati."},
            "type": "COMMENTED"
        },
        {
            "user": {"username": "budi_wani", "name": "Budi Wani", "followers_count": 500},
            "post": {"id": "post_laka_merr", "caption": "Kecelakaan tunggal di MERR, harap hati-hati."},
            "type": "LIKED"
        },

        # --- KELOMPOK 3: EVENT JAZZ TRAFFIC (Hype) ---
        {
            "user": {"username": "jazz_lover", "name": "Jazzy Dude", "followers_count": 890},
            "post": {"id": "post_jazz_traffic", "caption": "Lineup fase pertama Jazz Traffic Festival 2025!"},
            "type": "COMMENTED"
        },
        {
            "user": {"username": "surabaya_foodie", "name": "Kuliner Suroboyo", "followers_count": 15000},
            "post": {"id": "post_jazz_traffic", "caption": "Lineup fase pertama Jazz Traffic Festival 2025!"},
            "type": "LIKED"
        },
        {
            "user": {"username": "mahasiswa_its", "name": "Anak Teknik", "followers_count": 2300},
            "post": {"id": "post_jazz_traffic", "caption": "Lineup fase pertama Jazz Traffic Festival 2025!"},
            "type": "COMMENTED"
        },
        # Interaksi User Unik Lainnya
        {
            "user": {"username": "test_user_1", "name": "Test User 1", "followers_count": 100},
            "post": {"id": "post_macet_waru", "caption": "..."},
            "type": "COMMENTED"
        },
        {
            "user": {"username": "test_user_2", "name": "Test User 2", "followers_count": 200},
            "post": {"id": "post_macet_waru", "caption": "..."},
            "type": "COMMENTED"
        }
    ]

    count = 0
    for item in dummy_data:
        success = controller.add_interaction(item['user'], item['post'], item['type'])
        if success:
            count += 1
            
    return {"status": "success", "message": f"Berhasil memasukkan {count} data dummy ke Graph."}