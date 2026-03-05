from fastapi import APIRouter
from app.controllers import neo4j_migration_controller

router = APIRouter(
    prefix="/neo4j",
    tags=["Neo4j Migration"]
)

@router.post("/migrate")
async def start_migration_streaming():
    """
    Endpoint ini akan menampilkan progress langsung (1/50000, 2/50000, dst)
    di dalam body response saat di-hit.
    """
    return await neo4j_migration_controller.run_migration_streaming()

# ENDPOINT BARU UNTUK MENGHAPUS SEMUA DATA
@router.delete("/clear-all")
async def clear_all_neo4j_data():
    return await neo4j_migration_controller.delete_all_neo4j_data()