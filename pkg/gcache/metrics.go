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
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Distributed-cache metrics (enterprise-parity names; CONTRACT.md §8).
// Internal names are UNPREFIXED: mounts register them through JuiceFS's
// wrapped registerer (cmd/mount.go wrapRegister), which adds the "juicefs_"
// prefix and mp/vol labels at the metrics endpoint. When no registerer is
// provided (unit tests, standalone tools) they fall back to the default
// registry under the bare names below.
var (
	metricGets      prometheus.Counter
	metricBytes     prometheus.Counter
	metricErrors    prometheus.Counter
	metricDurations prometheus.Histogram
	metricFailures  *prometheus.GaugeVec
)

var metricsOnce sync.Once

// CollectMetrics registers the gcache metrics with reg (or the default
// registry if reg is nil). Safe to call multiple times; only the first call
// registers. Mounts should call it with their wrapped registerer so the
// series appear on the mount's metrics endpoint as juicefs_remotecache_*.
func CollectMetrics(reg prometheus.Registerer) {
	metricsOnce.Do(func() {
		metricGets = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotecache_gets",
			Help: "Count of distributed cache peer fetches",
		})
		metricBytes = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotecache_bytes",
			Help: "Bytes fetched from cache-group peers",
		})
		metricErrors = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "remotecache_errors",
			Help: "Count of failed distributed cache peer operations",
		})
		metricDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "remotecache_durations",
			Help:    "Distributed cache peer fetch latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 22),
		})
		metricFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "peer_failures",
			Help: "Consecutive failures per cache-group peer",
		}, []string{"peer"})
		if reg == nil {
			reg = prometheus.DefaultRegisterer
		}
		reg.MustRegister(metricGets, metricBytes, metricErrors, metricDurations, metricFailures)
	})
}

// observeFetch records one peer fetch outcome. Caller cancelation
// (speculative readahead torn down mid-flight) is not a peer health signal
// and is not counted.
func observeFetch(peer string, n int64, err error, seconds float64) {
	if metricGets == nil { // metrics not registered (pure-library use)
		return
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "operation was canceled") {
			return
		}
		metricErrors.Inc()
		return
	}
	metricGets.Inc()
	metricBytes.Add(float64(n))
	metricDurations.Observe(seconds)
}
