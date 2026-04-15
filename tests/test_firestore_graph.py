import pytest
from unittest.mock import patch
from app.controllers.firestore_graph_controller import create_graph_from_firestore

@pytest.mark.asyncio
@patch("app.controllers.post_controller.get_all_posts_from_db")
@patch("app.controllers.user_controller.get_all_users_from_db")
async def test_create_graph_from_firestore_success(mock_get_users, mock_get_posts):
    # Arrange
    mock_get_posts.return_value = [
        {"id": "post1", "accountName": "Budi", "title": "Berita 1", "reply_to_id": None},
        {"id": "post2", "accountName": "Andi", "title": "Balasan", "reply_to_id": "post1"}
    ]
    mock_get_users.return_value = [
        {"id": "u1", "nama": "Budi"},
        {"id": "u2", "nama": "Andi"}
    ]

    # Act
    result = await create_graph_from_firestore()

    # Assert
    assert "Graf berhasil dibuat" in result["message"]
    assert result["graph_info"]["nodes_count"] == 4 # 2 User + 2 Post
    assert result["graph_info"]["edges_count"] == 3 # 2 Authored + 1 Replied_to

@pytest.mark.asyncio
@patch("app.controllers.post_controller.get_all_posts_from_db")
async def test_create_graph_from_firestore_no_data(mock_get_posts):
    # Arrange
    mock_get_posts.return_value = [] # Simulasi DB kosong

    # Act & Assert
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        await create_graph_from_firestore()
    assert exc.value.status_code == 404