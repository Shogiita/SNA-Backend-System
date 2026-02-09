# GANTI DARI 3.10 MENJADI 3.11 untuk support library terbaru
FROM python:3.11.4

# Set working directory di dalam container
WORKDIR /code

# Install build dependencies (gcc, g++, cmake) 
# leidenalg/igraph sering membutuhkan kompilasi C core
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Salin requirements.txt terlebih dahulu
COPY requirements.txt .

# Upgrade pip dan install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Salin seluruh kode aplikasi
COPY ./app ./app

# Buat folder output
RUN mkdir -p generated_graphs

# Set environment variable port
ENV PORT=8000

# Command jalan
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]