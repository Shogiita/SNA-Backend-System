import pytest
from fastapi import HTTPException

from app.controllers.neo4j_graph_controller import _build_neo4j_graph_internal


def test_build_neo4j_graph_internal_invalid_mode():
    with pytest.raises(HTTPException) as excinfo:
        _build_neo4j_graph_internal(limit=10, mode=99)

    assert excinfo.value.status_code == 400
    assert "mode harus 1 atau 2" in excinfo.value.detail