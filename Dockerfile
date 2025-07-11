# ────────────────────────────────────────────────────────────────────────────────
#  Python base image
# ────────────────────────────────────────────────────────────────────────────────
FROM python:3.10-slim

# Workdir for your application
WORKDIR /app


# ────────────────────────────────────────────────────────────────────────────────
#  System-level build/runtime dependencies
# ────────────────────────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    cmake \
    ninja-build \
    libssl-dev \
    librocksdb-dev \
 && rm -rf /var/lib/apt/lists/*


# ────────────────────────────────────────────────────────────────────────────────
#  Build & install  ❱  liboqs  (shared library)
# ────────────────────────────────────────────────────────────────────────────────
RUN git clone --depth 1 https://github.com/open-quantum-safe/liboqs /tmp/liboqs \
 && cmake -S /tmp/liboqs -B /tmp/liboqs/build \
        -GNinja \
        -DBUILD_SHARED_LIBS=ON \
 && cmake --build /tmp/liboqs/build --parallel $(nproc) \
 && cmake --install /tmp/liboqs/build \
 && rm -rf /tmp/liboqs \
 && ldconfig          # refresh linker cache

# Make sure the shared object is always found at runtime
ENV LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}


# ────────────────────────────────────────────────────────────────────────────────
#  Build & install  ❱  liboqs-python  (Python CFFI wrapper)
# ────────────────────────────────────────────────────────────────────────────────
RUN git clone --depth 1 https://github.com/open-quantum-safe/liboqs-python /tmp/liboqs-python \
 && pip install /tmp/liboqs-python \
 && rm -rf /tmp/liboqs-python


# ────────────────────────────────────────────────────────────────────────────────
#  Project-specific Python dependencies
# ────────────────────────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt



# ────────────────────────────────────────────────────────────────────────────────
#  Copy application code *after* deps for better Docker caching
# ────────────────────────────────────────────────────────────────────────────────
COPY . .


# ────────────────────────────────────────────────────────────────────────────────
#  Runtime environment & ports
# ────────────────────────────────────────────────────────────────────────────────
ENV WALLET_PASSWORD=your_wallet_password
ENV ROCKSDB_PATH=/app/db
ENV RPC_PORT=8332
ENV GRAFANA=true
ENV GRAFANA_PORT=443

EXPOSE 8080/tcp 8332/tcp 8001/udp 8002/tcp


# ────────────────────────────────────────────────────────────────────────────────
#  Entrypoint
# ────────────────────────────────────────────────────────────────────────────────
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
