# Pachyderm exporter for Prometheus

[![Build Status](https://travis-ci.org/button/pachyderm_exporter.svg?branch=master)](https://travis-ci.org/button/pachyderm_exporter)
[![Go Report Card](https://goreportcard.com/badge/github.com/button/pachyderm_exporter)](https://goreportcard.com/report/github.com/button/pachyderm_exporter)

This is a simple server that tails Pachyderm's job list and exports stats via HTTP for Prometheus consumption.

## Getting started

The exporter connects to Pachyderm using the Pachyderm Go Client, via gRPC.

To run locally:

```
go build .
pachctl port-forward &
./pachyderm_exporter --pachyderm.address=localhost:30650
```

The exporter runs on port 9425 by default.

```
curl localhost:9425/metrics
```

## Exported Metrics

See [here](./exporter/exporter.go#L61) for the list of exported metrics.

## Kubernetes

Pachyderm runs on Kubernetes, so `pachyderm_exporter` should as well.

To get the Pachyderm `pachd` server address:

```
kubectl get service pachd -o go-template='{{.spec.clusterIP}}:{{(index .spec.ports 0).nodePort}}'
```

Then to run:

**TODO**
