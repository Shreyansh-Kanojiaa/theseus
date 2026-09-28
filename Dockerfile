FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /out/ ./cmd/...

# alpine rather than scratch: the testbed needs a shell inside, and chaos
# faults run this image as a helper for tc, iptables and fallocate.
FROM alpine:3.22
RUN apk add --no-cache iproute2 iptables
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["theseus-agent"]
