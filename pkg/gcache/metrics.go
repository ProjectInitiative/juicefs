/*
 * JuiceFS, Copyright 2026 ProjectInitiative, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gcache

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics, enterprise-parity names (CONTRACT.md §8). Package
// counters are registered here once; JuiceFS serves them from the mount's
// metrics endpoint alongside the other juicefs_* series.
var (
	metricGets      prometheus.Counter
	metricBytes     prometheus.Counter
	metricErrors    prometheus.Counter
	metricDurations prometheus.Histogram
	metricFailures  *prometheus.GaugeVec
)

func init() {
	metricGets = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "juicefs_remotecache_gets",
		Help: "Count of distributed cache peer fetches",
	})
	metricBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "juicefs_remotecache_bytes",
		Help: "Bytes fetched from cache-group peers",
	})
	metricErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "juicefs_remotecache_errors",
		Help: "Count of failed distributed cache peer operations",
	})
	metricDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "juicefs_remotecache_durations",
		Help:    "Distributed cache peer fetch latency in seconds",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 22),
	})
	metricFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "juicefs_peer_failures",
		Help: "Consecutive failures per cache-group peer",
	}, []string{"peer"})
	prometheus.MustRegister(metricGets, metricBytes, metricErrors, metricDurations, metricFailures)
}

// observeFetch records one peer fetch outcome.
func observeFetch(peer string, n int64, err error, seconds float64) {
	if err != nil {
		metricErrors.Inc()
		metricFailures.WithLabelValues(peer).Inc()
		return
	}
	metricGets.Inc()
	metricBytes.Add(float64(n))
	metricDurations.Observe(seconds)
}
