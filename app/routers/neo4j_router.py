from fastapi import APIRouter, BackgroundTasks
from app.controllers import neo4j_migration_controller

router = APIRouter(
    prefix="/neo4j",
    tags=["Neo4j Migration"]
)

#done
@router.get("/migrate")
async def start_migration_background(background_tasks: BackgroundTasks):
    """
    Menjalankan proses migrasi data dari Firebase ke Neo4j di latar belakang.
    API akan langsung mengembalikan response sukses.
    """
    background_tasks.add_task(neo4j_migration_controller.run_migration_background)
    
    return {
        "status": "success",
        "message": "Proses migrasi sedang berjalan di latar belakang. Silakan cek terminal server untuk melihat progres."
    }

@router.delete("/clear-all")
async def clear_all_neo4j_data():
    return await neo4j_migration_controller.delete_all_neo4j_data()