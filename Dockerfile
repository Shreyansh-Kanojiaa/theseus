FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /out/ ./cmd/...

# alpine rather than scratch: the testbed and chaos faults need a shell inside.
FROM alpine:3.22
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["theseus-agent"]
