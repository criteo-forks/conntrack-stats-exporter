//    This file is part of conntrack-stats-exporter.
//
//    conntrack-stats-exporter is free software: you can redistribute it and/or
//    modify it under the terms of the GNU General Public License as published
//    by the Free Software Foundation, either version 3 of the License, or (at
//    your option) any later version.
//
//    conntrack-stats-exporter is distributed in the hope that it will be
//    useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
//    Public License for more details.
//
//    You should have received a copy of the GNU General Public License along
//    with conntrack-stats-exporter.  If not, see
//    <http://www.gnu.org/licenses/>.

package exporter

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

const (
	promNamespace = "conntrack"
	promSubSystem = "stats"
)

var regex = regexp.MustCompile(`([a-z_]+)=(\d+)`)

var (
	conntrackStatsFound = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_found", promNamespace, promSubSystem),
		Help: "Total of conntrack found",
	}, []string{"cpu", "netns"})
	conntrackStatsInvalid = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_invalid", promNamespace, promSubSystem),
		Help: "Total of conntrack invalid",
	}, []string{"cpu", "netns"})
	conntrackStatsIgnore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_ignore", promNamespace, promSubSystem),
		Help: "Total of conntrack ignore",
	}, []string{"cpu", "netns"})
	conntrackStatsInsert = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_insert", promNamespace, promSubSystem),
		Help: "Total of conntrack insert",
	}, []string{"cpu", "netns"})
	conntrackStatsInsertFailed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_insert_failed", promNamespace, promSubSystem),
		Help: "Total of conntrack insert_failed",
	}, []string{"cpu", "netns"})
	conntrackStatsDrop = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_drop", promNamespace, promSubSystem),
		Help: "Total of conntrack drop",
	}, []string{"cpu", "netns"})
	conntrackStatsEarlyDrop = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_early_drop", promNamespace, promSubSystem),
		Help: "Total of conntrack early_drop",
	}, []string{"cpu", "netns"})
	conntrackStatsError = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_error", promNamespace, promSubSystem),
		Help: "Total of conntrack error",
	}, []string{"cpu", "netns"})
	conntrackStatsSearchRestart = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_search_restart", promNamespace, promSubSystem),
		Help: "Total of conntrack search_restart",
	}, []string{"cpu", "netns"})
	conntrackStatsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_%s_count", promNamespace, promSubSystem),
		Help: "Total of conntrack count",
	}, []string{"netns"})
	conntrackScrapeError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: fmt.Sprintf("%s_%s_scrape_error", promNamespace, promSubSystem),
		Help: "Total of conntrack count",
	}, []string{"netns"})
)

type metricList []map[string]int

// Exporter exports stats from the conntrack CLI. The metrics are named with
// prefix `conntrack_stats_*`.
type Exporter struct {
	listenAddr  string
	promHandler http.Handler
	netnsList   []string
}

// New creates a new conntrack stats exporter.
func New(listenAddr string, netnsList []string) *Exporter {
	e := &Exporter{
		listenAddr:  listenAddr,
		promHandler: promhttp.Handler(),
		netnsList:   netnsList,
	}
	return e
}

func (e *Exporter) Run() error {
	prometheus.MustRegister(conntrackStatsFound)
	prometheus.MustRegister(conntrackStatsInvalid)
	prometheus.MustRegister(conntrackStatsIgnore)
	prometheus.MustRegister(conntrackStatsInsert)
	prometheus.MustRegister(conntrackStatsInsertFailed)
	prometheus.MustRegister(conntrackStatsDrop)
	prometheus.MustRegister(conntrackStatsEarlyDrop)
	prometheus.MustRegister(conntrackStatsError)
	prometheus.MustRegister(conntrackStatsSearchRestart)
	prometheus.MustRegister(conntrackStatsCount)
	prometheus.MustRegister(conntrackScrapeError)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", e.metricsHandler)

	return http.ListenAndServe(e.listenAddr, mux)
}

func (e *Exporter) metricsHandler(rw http.ResponseWriter, r *http.Request) {
	e.update()
	e.promHandler.ServeHTTP(rw, r)
}

// update get conntrack stats and update prometheus metrics.
func (e *Exporter) update() {
	metricsPerNetns := make(map[string]metricList)
	var err error
	for _, netns := range e.netnsList {
		metricsPerNetns[netns], err = getMetrics(netns)
		if err != nil {
			conntrackScrapeError.WithLabelValues(netns).Inc()
			log.Errorf("failed to get conntrack metrics netns: %s", err)
		}
		for _, metrics := range metricsPerNetns[netns] {
			cpu := strconv.Itoa(metrics["cpu"])
			for metricName, value := range metrics {
				switch metricName {
				case "found":
					conntrackStatsFound.WithLabelValues(cpu, netns).Set(float64(value))
				case "invalid":
					conntrackStatsInvalid.WithLabelValues(cpu, netns).Set(float64(value))
				case "ignore":
					conntrackStatsIgnore.WithLabelValues(cpu, netns).Set(float64(value))
				case "insert":
					conntrackStatsInsert.WithLabelValues(cpu, netns).Set(float64(value))
				case "insert_failed":
					conntrackStatsInsertFailed.WithLabelValues(cpu, netns).Set(float64(value))
				case "drop":
					conntrackStatsDrop.WithLabelValues(cpu, netns).Set(float64(value))
				case "early_drop":
					conntrackStatsEarlyDrop.WithLabelValues(cpu, netns).Set(float64(value))
				case "error":
					conntrackStatsError.WithLabelValues(cpu, netns).Set(float64(value))
				case "search_restart":
					conntrackStatsSearchRestart.WithLabelValues(cpu, netns).Set(float64(value))
				case "count":
					conntrackStatsCount.WithLabelValues(netns).Set(float64(value))
				default:
				}
			}
		}

	}
}

func getMetrics(netns string) (metricList, error) {
	var lines []string
	var total string
	var err error
	err = execInNetns(netns, func() error {
		lines, err = getConntrackStats()
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get conntrack stats: %s", err)
	}
	err = execInNetns(netns, func() error {
		total, err = getConntrackCounter()
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get conntrack counter: %s", err)
	}
	lines = append(lines, total)
	metrics := make(metricList, len(lines))
ParseEachOutputLine:
	for _, line := range lines {
		matches := regex.FindAllStringSubmatch(line, -1)
		if matches == nil {
			continue ParseEachOutputLine
		}
		metric := make(map[string]int)
		for _, match := range matches {
			if len(match) != 3 {
				return nil, fmt.Errorf("len(%v) != 3", match)
			}
			key, v := match[1], match[2]
			value, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("some key=value has a non integer value: %q", line)
			}
			metric[key] = value
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

func getConntrackCounter() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3e9)
	defer cancel()

	cmd := exec.CommandContext(ctx, "conntrack", "--count")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error happened while calling conntrack: %s", err)
	}

	return fmt.Sprintf("count=%s", out), nil
}

func getConntrackStats() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3e9)
	defer cancel()

	cmd := exec.CommandContext(ctx, "conntrack", "--stats")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("error happened while calling conntrack: %s", err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if scanner.Err() != nil {
		return nil, fmt.Errorf("error while parsing conntrack output: %s", err)
	}
	return lines, nil
}
