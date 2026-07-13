# syntax=docker/dockerfile:1

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod ./
COPY go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/streamer ./streamer/cmd/streamer

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/streamer /streamer
# Bundle the sample CSV so the streamer can run out-of-the-box in kind.
COPY data/dcgm_metrics_sample.csv /data/dcgm_metrics_sample.csv
USER nonroot:nonroot
ENTRYPOINT ["/streamer"]
