from fastapi import APIRouter, BackgroundTasks
from app.controllers import neo4j_migration_controller

router = APIRouter(
    prefix="/neo4j",
    tags=["Neo4j Migration"]
)


@router.post("/migrate")
async def start_migration_background(background_tasks: BackgroundTasks):
    started = neo4j_migration_controller.start_migration(background_tasks)

    if not started:
        return {
            "status": "running",
            "message": "Migrasi masih dianggap berjalan. Cek /neo4j/migrate/status. Jika stuck, hit POST /neo4j/migrate/unlock."
        }

    return {
        "status": "success",
        "message": "Migrasi Firebase ke Neo4j dimulai. Cek terminal atau /neo4j/migrate/status."
    }


@router.get("/migrate/status")
async def get_migration_status():
    return neo4j_migration_controller.get_migration_status()


@router.post("/migrate/unlock")
async def unlock_migration():
    return neo4j_migration_controller.unlock_migration()


@router.delete("/clear-all")
async def clear_all_neo4j_data():
    return await neo4j_migration_controller.delete_all_neo4j_data()