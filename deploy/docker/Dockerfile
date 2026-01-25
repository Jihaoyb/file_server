FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    cmake \
    ninja-build \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Build steps assume vcpkg is preconfigured on host or in CI. Placeholder for now.
