FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++=4:12.2.0-3 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY vendor/ vendor/
COPY src/ src/
RUN g++ -std=c++17 -O3 -DNDEBUG -static-libstdc++ -static-libgcc -Ivendor src/main.cpp -o /minivectordb
FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
COPY --from=build /minivectordb /usr/local/bin/minivectordb
ENV PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["/usr/local/bin/minivectordb","--data-dir","/data"]
