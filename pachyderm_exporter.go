package main

import (
	"log"

	"net/http"

	"github.com/button/pachyderm_exporter/exporter"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		listenAddress    = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9425").String()
		metricsPath      = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		pachydermAddress = kingpin.Flag("pachyderm.address", "Address on which to scrape Pachyderm").String()
		pachydermTimeout = kingpin.Flag("pachyderm.timeout", "Timeout for getting job information Pachyderm").Default("5s").Duration()
	)

	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	c, err := client.NewFromAddress(*pachydermAddress)
	if err != nil {
		log.Fatal(err)
	}
	exp := exporter.New(c, *pachydermTimeout)
	prometheus.MustRegister(exp)

	log.Printf("Starting pachyderm_exporter on port %s", *listenAddress)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Pachyderm Exporter</title></head>
             <body>
             <h1>Pachyderm Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
