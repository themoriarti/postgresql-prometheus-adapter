# Multi-stage build; replaces the original centos:7 image (EOL).
FROM docker.io/library/golang:1.22 AS build
ARG VERSION=development
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION}" \
      -o /postgresql-prometheus-adapter .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /postgresql-prometheus-adapter /postgresql-prometheus-adapter
EXPOSE 9201
USER nonroot:nonroot
ENTRYPOINT ["/postgresql-prometheus-adapter"]
