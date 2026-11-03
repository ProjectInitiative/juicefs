package gcache

// Stub for builds without the `rdma` tag: the native verbs transport is
// unavailable (compiles everywhere, needs no RDMA headers).

func init() {
	verbsDialFn = nil
	verbsListenFn = nil
}
