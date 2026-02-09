# Gunakan image Python yang ringan namun memiliki dependensi build dasar (slim)
FROM python:3.10-slim

# Set working directory di dalam container
WORKDIR /code

# Install build dependencies yang mungkin dibutuhkan oleh igraph/leidenalg
# (gcc, g++ dibutuhkan untuk kompilasi beberapa library C)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Salin requirements.txt terlebih dahulu untuk memanfaatkan caching layer Docker
COPY requirements.txt .

# Install dependencies Python
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Salin seluruh kode aplikasi ke dalam container
# Folder 'app' disalin ke /code/app
COPY ./app ./app

# Salin file-file pendukung yang dibutuhkan oleh controller
# (Sesuai kode di csv_graph_controller.py dan database.py)
COPY serviceAccountKey.json .
COPY twitter_dataset.csv .

# Buat folder untuk output generate graph agar tidak error saat runtime
# (Sesuai kode di sna_controller.py)
RUN mkdir -p generated_graphs

# Set environment variable untuk port (Cloud Run default menggunakan 8080)
ENV PORT=8080

# Command untuk menjalankan aplikasi saat container start
# Kita menggunakan host 0.0.0.0 agar bisa diakses dari luar container
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]