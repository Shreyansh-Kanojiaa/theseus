FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /theseus-agent ./cmd/theseus-agent

# alpine rather than scratch: the testbed and chaos faults need a shell inside.
FROM alpine:3.22
COPY --from=build /theseus-agent /usr/local/bin/theseus-agent
ENTRYPOINT ["theseus-agent"]
