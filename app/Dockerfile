FROM golang:1.20.6-alpine3.18

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN go build -o app .

ENTRYPOINT ["/build/app"]
