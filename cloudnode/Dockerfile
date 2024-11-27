FROM golang:1.22-alpine AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

RUN apk add --no-cache git

ENV GOPROXY=direct

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o /app/cloudcore main.go

FROM scratch

COPY --from=builder /app/cloudcore .

EXPOSE 50051

CMD ["./cloudcore"]
