FROM golang:1.20

WORKDIR /usr/src/app

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN go build -v -o /usr/local/bin/semadb ./server

EXPOSE 8080

CMD ["go", "run", "./server"]

