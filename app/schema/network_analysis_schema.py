from typing import Dict


EDGE_WEIGHT_SCHEMA: Dict[str, Dict[str, object]] = {
    "LIKE": {
        "weight": 1,
        "description": "Interaksi ringan berupa like pada konten."
    },
    "MENTION": {
        "weight": 2,
        "description": "Interaksi dari komentar atau reply yang mengandung pola @username."
    },
    "COMMENT": {
        "weight": 3,
        "description": "Interaksi aktif berupa komentar pada konten."
    },
    "REPLY": {
        "weight": 4,
        "description": "Interaksi percakapan langsung berupa balasan komentar."
    },
    "POST_OR_AUTHORED": {
        "weight": 5,
        "description": "Relasi pembentukan konten antara user dan post pada graf user-post."
    }
}