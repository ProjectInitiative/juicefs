# gcache-enabled JuiceFS image for the CSI driver (multi-arch, built
# natively per-arch: amd64 on astrolabe, arm64 on chronometer — no QEMU).
#
# Differences from juicedata/mount:ce-*:
#   - /bin/mount.juicefs symlink: the CSI mount script execs
#     /bin/mount.juicefs ${metaurl} ... -o ..., so the binary must exist
#     at that path (upstream mount images ship it; a plain juicefs build
#     does not).
#   - fuse3 + curl + psql-client + bash for the driver's mount script and
#     gcache's SQL registry probes.
#
# After building per-arch, assemble a manifest list:
#   docker manifest create 172.16.1.50:31872/gcache-juicefs:v1.5.0-ring.1 \
#     -a 172.16.1.50:31872/gcache-juicefs:v1.5.0-ring.1-amd64 \
#     -a 172.16.1.50:31872/gcache-juicefs:v1.5.0-ring.1-arm64
#   docker manifest push 172.16.1.50:31872/gcache-juicefs:v1.5.0-ring.1
FROM golang:1.25-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev make git && apt-get clean
WORKDIR /src
COPY . .
RUN CGO_ENABLED=1 go build -o /out/juicefs .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends fuse3 ca-certificates curl bash postgresql-client && apt-get clean && echo user_allow_other >> /etc/fuse.conf
COPY --from=build /out/juicefs /usr/local/bin/juicefs
RUN chmod +x /usr/local/bin/juicefs \
  && ln -sf /usr/local/bin/juicefs /bin/mount.juicefs \
  && juicefs --version && /bin/mount.juicefs --version
