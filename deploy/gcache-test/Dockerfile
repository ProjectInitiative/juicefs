# gcache-test image: JuiceFS fork with distributed cache group support.
# Build context must be the repo root:
#   docker build -f deploy/gcache-test/Dockerfile -t gcache-juicefs:dev .
FROM golang:1.25-alpine AS build
RUN apk add --no-cache gcc musl-dev make git bash
WORKDIR /src
COPY . .
RUN CGO_ENABLED=1 go build -o /out/juicefs .

FROM alpine:3.20
RUN apk add --no-cache fuse3 ca-certificates curl bash postgresql-client && \
    echo "user_allow_other" >> /etc/fuse.conf
COPY --from=build /out/juicefs /usr/local/bin/juicefs
# smoke entry: starts two cache-group mounts + verifier (see run-smoke.sh)
COPY hack/gcache-smoke-test.sh /usr/local/bin/gcache-smoke-test.sh
RUN chmod +x /usr/local/bin/gcache-smoke-test.sh
ENTRYPOINT ["/usr/local/bin/juicefs"]
