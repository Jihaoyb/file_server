FROM ubuntu:24.04 AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cmake curl g++ git make ninja-build pkg-config tar unzip zip \
    && rm -rf /var/lib/apt/lists/*

# Match the vcpkg baseline in vcpkg.json and CI.
ARG VCPKG_COMMIT=01e159b519b7e791cc5bb3548663a26d9c0922a3
ENV VCPKG_ROOT=/opt/vcpkg
ENV VCPKG_MAX_CONCURRENCY=2

RUN git init "${VCPKG_ROOT}" \
    && git -C "${VCPKG_ROOT}" remote add origin https://github.com/microsoft/vcpkg.git \
    && git -C "${VCPKG_ROOT}" fetch origin "${VCPKG_COMMIT}" \
    && git -C "${VCPKG_ROOT}" checkout --detach FETCH_HEAD \
    && "${VCPKG_ROOT}/bootstrap-vcpkg.sh" -disableMetrics

WORKDIR /src
COPY vcpkg.json ./
RUN --mount=type=cache,target=/opt/vcpkg/downloads \
    --mount=type=cache,target=/root/.cache/vcpkg/archives \
    "${VCPKG_ROOT}/vcpkg" install

COPY CMakeLists.txt CMakePresets.json ./
COPY include/ include/
COPY src/ src/
COPY web/ web/

RUN cmake --preset release -DVCPKG_INSTALLED_DIR=/src/vcpkg_installed -DVCPKG_MANIFEST_INSTALL=OFF \
    && cmake --build --preset release --target nebulafs --parallel 2

FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 nebulafs \
    && useradd --uid 10001 --gid 10001 --no-create-home nebulafs \
    && install -d -o nebulafs -g nebulafs /app/data

WORKDIR /app
COPY --from=build /src/build/release/nebulafs /usr/local/bin/nebulafs
COPY config/database.json config/database.json
COPY LICENSE /usr/share/doc/nebulafs/LICENSE
USER nebulafs
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/nebulafs"]
CMD ["--config", "/run/nebulafs/server.json", "--database", "/app/config/database.json"]
