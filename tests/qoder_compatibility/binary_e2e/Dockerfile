FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends python3 bash ripgrep ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV LANG=C LC_ALL=C HOME=/root WORK_DIR=/data
ENTRYPOINT []
