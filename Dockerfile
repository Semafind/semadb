FROM golang:1.22.2-bookworm as build
LABEL org.opencontainers.image.description "No fuss multi-index hybrid vector database / search engine"

WORKDIR /app

COPY . .

# Download go modules
RUN go mod download && go mod verify

RUN go build -v -o /semadb ./

# FROM gcr.io/distroless/static-debian12
FROM debian:bookworm-slim

COPY --from=build /semadb /

EXPOSE 8080 9898

CMD ["/semadb"]