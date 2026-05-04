from fastapi import Header, HTTPException
from firebase_admin import auth as firebase_auth
from firebase_admin import credentials
import firebase_admin
import os
import json


def initialize_firebase_admin() -> None:
    """
    Inisialisasi Firebase Admin SDK.

    Mendukung:
    1. FIREBASE_SERVICE_ACCOUNT_JSON
    2. FIREBASE_SERVICE_ACCOUNT_PATH
    3. FIREBASE_* per-field dari .env
    4. Default credentials sebagai fallback
    """
    if firebase_admin._apps:
        return

    service_account_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")

    try:
        # 1. Jika pakai JSON utuh di env
        if service_account_json:
            service_account_info = json.loads(service_account_json)

            if "private_key" in service_account_info:
                service_account_info["private_key"] = service_account_info[
                    "private_key"
                ].replace("\\n", "\n")

            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)

            print("[FIREBASE] Initialized using FIREBASE_SERVICE_ACCOUNT_JSON")
            print(f"[FIREBASE] project_id: {service_account_info.get('project_id')}")
            return

        # 2. Jika pakai path file JSON
        if service_account_path:
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)

            print("[FIREBASE] Initialized using FIREBASE_SERVICE_ACCOUNT_PATH")
            print(f"[FIREBASE] path: {service_account_path}")
            return

        # 3. Jika pakai env satu-satu seperti FIREBASE_PROJECT_ID, FIREBASE_PRIVATE_KEY, dst
        firebase_project_id = os.getenv("FIREBASE_PROJECT_ID")
        firebase_private_key = os.getenv("FIREBASE_PRIVATE_KEY")
        firebase_client_email = os.getenv("FIREBASE_CLIENT_EMAIL")

        if firebase_project_id and firebase_private_key and firebase_client_email:
            service_account_info = {
                "type": os.getenv("FIREBASE_TYPE", "service_account"),
                "project_id": firebase_project_id,
                "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
                "private_key": firebase_private_key.replace("\\n", "\n"),
                "client_email": firebase_client_email,
                "client_id": os.getenv("FIREBASE_CLIENT_ID"),
                "auth_uri": os.getenv(
                    "FIREBASE_AUTH_URI",
                    "https://accounts.google.com/o/oauth2/auth",
                ),
                "token_uri": os.getenv(
                    "FIREBASE_TOKEN_URI",
                    "https://oauth2.googleapis.com/token",
                ),
                "auth_provider_x509_cert_url": os.getenv(
                    "FIREBASE_AUTH_PROVIDER_CERT_URL",
                    "https://www.googleapis.com/oauth2/v1/certs",
                ),
                "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_CERT_URL"),
            }

            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)

            print("[FIREBASE] Initialized using FIREBASE_* environment variables")
            print(f"[FIREBASE] project_id: {firebase_project_id}")
            print(f"[FIREBASE] client_email: {firebase_client_email}")
            return

        # 4. Fallback
        firebase_admin.initialize_app()
        print("[FIREBASE] Initialized using default credentials")

    except Exception as e:
        print("[FIREBASE INIT ERROR]", str(e))
        raise e


initialize_firebase_admin()


def get_current_admin(authorization: str | None = Header(None)) -> dict:
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization token tidak ditemukan.",
        )

    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Format Authorization tidak valid. Gunakan Bearer token.",
        )

    token = authorization.replace("Bearer ", "").strip()

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authorization token kosong.",
        )

    try:
        decoded = firebase_auth.verify_id_token(
            token,
            clock_skew_seconds=10
        )
    except Exception as e:
        print("[FIREBASE TOKEN ERROR]", str(e))
        raise HTTPException(
            status_code=401,
            detail=f"Token Firebase tidak valid: {str(e)}",
        )

    is_admin = decoded.get("admin") is True or decoded.get("admin") == "true"

    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail="Akun tidak memiliki akses admin.",
        )

    return {
        "uid": decoded.get("uid"),
        "email": decoded.get("email"),
        "name": decoded.get("name"),
        "claims": decoded,
    }
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization token tidak ditemukan.",
        )

    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Format Authorization tidak valid. Gunakan Bearer token.",
        )

    token = authorization.replace("Bearer ", "").strip()

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authorization token kosong.",
        )

    try:
        decoded = firebase_auth.verify_id_token(
            token,
            clock_skew_seconds=10
        )
    except Exception as e:
        print("[FIREBASE TOKEN ERROR]", str(e))
        raise HTTPException(
            status_code=401,
            detail=f"Token Firebase tidak valid: {str(e)}",
        )

    is_admin = decoded.get("admin") is True or decoded.get("admin") == "true"

    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail="Akun tidak memiliki akses admin.",
        )

    return {
        "uid": decoded.get("uid"),
        "email": decoded.get("email"),
        "name": decoded.get("name"),
        "claims": decoded,
    }