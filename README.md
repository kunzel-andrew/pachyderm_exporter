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

To deploy the exporter, execute the following command:

```
kubectl apply -f https://raw.githubusercontent.com/button/pachyderm_exporter/master/deploy/pachyderm-exporter.yaml
```

The exporter should be able to discover pachyderm and start successfully. Find its pod and service by running:

```
> kubectl get po,svc -l app=pachyderm-exporter
NAME                                     READY     STATUS    RESTARTS   AGE
po/pachyderm-exporter-56d7bf784f-w4kpc   1/1       Running   0          4m

NAME                     TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)    AGE
svc/pachyderm-exporter   ClusterIP   100.66.52.64   <none>        9425/TCP   11m
```

Here, the exporter can be reached within the Kubernetes cluster at `100.66.52.64` on port `9425`.

You should be able to scrape metrics manually by executing:

```
kubectl run curl --image=radial/busyboxplus:curl -i --tty --rm --restart=Never -- curl pachyderm-exporter.default.svc.cluster.local:9425/metrics
```
