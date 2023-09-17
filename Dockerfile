FROM golang:1.21

WORKDIR /app

# Download go modules
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN go build -v -o /semadb ./

EXPOSE 8080 9898

CMD ["/semadb"]

