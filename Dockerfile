# Stage 1: Build
FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /rdf-index-elasticsearch .

# Stage 2: Final minimal image
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /rdf-index-elasticsearch /usr/local/bin/rdf-index-elasticsearch

ENTRYPOINT ["rdf-index-elasticsearch"]
