from fastapi import APIRouter, BackgroundTasks, Query
from app.controllers import neo4j_migration_controller

router = APIRouter(
    prefix="/neo4j",
    tags=["Neo4j Migration"]
)


@router.post("/migrate")
async def start_migration_background(
    background_tasks: BackgroundTasks,
    force_full: bool = Query(
        False,
        description="Set True jika ingin migrasi ulang semua data. Default False = hanya migrasi data baru/berubah."
    )
):
    """
    Menjalankan proses migrasi data dari Firebase ke Neo4j di background.

    Default:
    - incremental migration
    - tidak menghapus data lama
    - tidak membuat data double
    """
    started = neo4j_migration_controller.start_migration(
        background_tasks=background_tasks,
        force_full=force_full
    )

    if not started:
        return {
            "status": "running",
            "message": "Proses migrasi masih berjalan. Tunggu sampai selesai sebelum menjalankan migrasi baru."
        }

    return {
        "status": "success",
        "mode": "full" if force_full else "incremental",
        "message": "Proses migrasi Firebase ke Neo4j sedang berjalan di background. Silakan cek /neo4j/migrate/status."
    }


@router.get("/migrate/status")
async def get_migration_status():
    """
    Melihat status migrasi Firebase ke Neo4j.
    """
    return neo4j_migration_controller.get_migration_status()


@router.delete("/clear-all")
async def clear_all_neo4j_data():
    """
    Menghapus semua data Neo4j dan reset metadata migrasi.
    """
    return await neo4j_migration_controller.delete_all_neo4j_data()