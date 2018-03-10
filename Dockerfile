FROM golang:1.10.0-alpine as builder
COPY . /go/src/github.com/button/pachyderm_exporter/
RUN CGO_ENABLED=0 go install github.com/button/pachyderm_exporter

FROM alpine:3.7
RUN apk --no-cache add ca-certificates
COPY --from=builder /go/bin/pachyderm_exporter /usr/local/bin/pachyderm_exporter
EXPOSE 9425
CMD ["pachyderm_exporter"]
