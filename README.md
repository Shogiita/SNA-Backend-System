# SNA-Backend-System

A backend system for Social Network Analysis (SNA) built with FastAPI.

### 1. Create and Activate Virtual Environment

# Create venv
# # python -m venv venv

# Activate venv (Windows)
# # .\venv\Scripts\activate

# Activate venv (macOS / Linux)
# # source venv/bin/activate

### 2. Install Libraries

# Install all required libraries
# # pip install fastapi "uvicorn[standard]" networkx python-leidenalg igraph firebase-admin

### 3. Firebase Configuration

# Make sure the serviceAccountKey.json file is in the project's root directory.

### 4. Run Program

# Run the server with uvicorn
# # uvicorn app.main:app --reload

### 5. API Endpoints

* **`GET /`**
    * Welcome message from the API.

* **`GET /graph/generate`**
    * Creates a graph from dummy data, calculates betweenness centrality, and detects communities (Leiden).

* **`GET /graph/generate/pajek`**
    * Creates a graph from dummy data in Pajek (`.net`) text format.

* **`GET /ssgraph`**
    * Creates a graph from the `users` and `kawanss` collections in Firestore.

* **`GET /posts/all`**
    * Fetches all posts from the `kawanss` collection in Firestore.

* **`POST /users/`**
    * Creates a new user in the `users` collection.

* **`GET /users/all`**
    * Fetches all users from the `users` collection.

* **`GET /users/{user_id}`**
    * Fetches a specific user based on `user_id`.