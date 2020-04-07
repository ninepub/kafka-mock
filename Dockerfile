FROM golang:1.13-alpine3.10 as build

COPY . /go/src/github.com/ninepub/kafka-mock

WORKDIR /go/src/github.com/ninepub/kafka-mock

RUN go build -o kafka-mock cmd/main.go

FROM alpine:3.10 AS release

COPY --from=build /go/src/github.com/ninepub/kafka-mock/kafka-mock /go/bin/kafka-mock

ENTRYPOINT ["/go/bin/kafka-mock"]
