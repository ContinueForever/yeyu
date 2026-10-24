# Single-node durable development image. For local development prefer scripts/build.sh.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ cmake ninja-build protobuf-compiler libprotobuf-dev \
    libgtest-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DYEWUKV_BUILD_TESTS=OFF && cmake --build build -j"$(nproc)"

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    libprotobuf32t64 libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/bin/yewukv-server /usr/local/bin/yewukv-server
COPY --from=build /src/build/bin/yewukv-client /usr/local/bin/yewukv-client
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/yewukv-server", "--port", "9000"]
