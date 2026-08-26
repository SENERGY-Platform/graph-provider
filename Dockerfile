FROM golang:1.26 AS builder

# Handed in by .github/workflows/prod.yml as the tag that run created.
# git describe cannot supply it: CI checks out shallow and without tags.
ARG VERSION=dev

COPY . /go/src/app
WORKDIR /go/src/app

RUN go generate ./...
RUN CGO_ENABLED=0 go build -o /go/bin/app -ldflags "-X main.version=$VERSION" .

FROM alpine:3.22

RUN apk --no-cache add ca-certificates
COPY --from=builder /go/bin/app /opt/app
COPY --from=builder /go/src/app/config.json /opt/config.json

WORKDIR /opt
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD wget -q -O /dev/null http://localhost:8080/health || exit 1

ENTRYPOINT ["/opt/app", "-config", "/opt/config.json"]
