from fastapi import Header, HTTPException
from firebase_admin import auth as firebase_auth
import firebase_admin
from firebase_admin import credentials
import os

if not firebase_admin._apps:
    cred_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    if cred_path:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()

def get_current_admin(authorization: str | None = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization token tidak ditemukan.")

    token = authorization.replace("Bearer ", "").strip()

    try:
        decoded = firebase_auth.verify_id_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Token Firebase tidak valid.")

    is_admin = decoded.get("admin") is True or decoded.get("admin") == "true"
    if not is_admin:
        raise HTTPException(status_code=403, detail="Akun tidak memiliki akses admin.")

    return {
        "uid": decoded.get("uid"),
        "email": decoded.get("email"),
        "name": decoded.get("name"),
        "claims": decoded,
    }