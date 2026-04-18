# NATS + ClickHouse must be reachable from this container (see docker-compose / deploy README).
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/backtest-engine ./cmd/worker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/backtest-engine .
EXPOSE 8090
ENV BT_HTTP_PORT=8090
CMD ["./backtest-engine"]
