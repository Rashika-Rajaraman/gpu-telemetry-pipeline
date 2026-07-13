# syntax=docker/dockerfile:1

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod ./
COPY go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/broker ./message-queue/cmd

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/broker /broker
EXPOSE 9000
USER nonroot:nonroot
ENTRYPOINT ["/broker"]
