from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from app.controllers import neo4j_migration_controller

router = APIRouter(
    prefix="/neo4j",
    tags=["Neo4j Migration"]
)

@router.get("/migrate")
async def start_migration_streaming():
    """
    Endpoint SSE untuk Frontend.
    Gunakan EventSource("http://url/neo4j/migrate") di JS/Flutter untuk membaca progress.
    """
    return StreamingResponse(
        neo4j_migration_controller.run_migration_streaming(),
        media_type="text/event-stream"
    )

@router.delete("/clear-all")
async def clear_all_neo4j_data():
    return await neo4j_migration_controller.delete_all_neo4j_data()