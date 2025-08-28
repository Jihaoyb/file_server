# Multi-stage build for File Server
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    curl \
    zip \
    unzip \
    tar \
    ninja-build \
    && rm -rf /var/lib/apt/lists/*

# Install vcpkg
RUN git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh

# Set vcpkg environment
ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="$VCPKG_ROOT:$PATH"

# Copy source code
WORKDIR /app
COPY . .

# Install dependencies
RUN vcpkg install --triplet=x64-linux

# Build the application
RUN mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release \
             -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
             -DBUILD_TESTS=OFF \
             -DENABLE_LTO=ON \
             .. \
    && cmake --build . --config Release -j$(nproc)

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Create app user
RUN useradd -r -s /bin/false fileserver

# Create directories
RUN mkdir -p /app/bin /app/config /app/data /app/logs \
    && chown -R fileserver:fileserver /app

# Copy binary and configuration
COPY --from=builder /app/build/bin/file_server /app/bin/
COPY --from=builder /app/config/ /app/config/
COPY --from=builder /app/scripts/ /app/scripts/

# Set permissions
RUN chmod +x /app/bin/file_server /app/scripts/*.sh

# Switch to app user
USER fileserver

# Set working directory
WORKDIR /app

# Expose port
EXPOSE 8443

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f -k https://localhost:8443/health || exit 1

# Default command
CMD ["./bin/file_server", "--config", "config/server.json"]