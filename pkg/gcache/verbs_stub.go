//go:build !rdma

package gcache

// Stub for builds without the `rdma` tag: the native verbs transport is
// unavailable (compiles everywhere, needs no RDMA headers).

func init() {
	verbsDialFn = nil
	verbsListenFn = nil
}

// Server-side conn accounting stubs (Phase 2b): the verbs registry lives in
// verbs_impl.go (rdma build); non-rdma builds always report zero.
func ServeLiveConns() int                       { return 0 }
func ServeLiveStats() (accepted, closed, live int64) { return 0, 0, 0 }

// ServeLifecycleMetrics stub: non-rdma builds have no verbs lifecycle.
func ServeLifecycleMetrics() (accepted, unregistered, destroyed, cleanupFailures int64, meanTeardownNs int64) {
	return 0, 0, 0, 0, 0
}
