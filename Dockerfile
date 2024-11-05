FROM golang:1.23.2-bookworm as build
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