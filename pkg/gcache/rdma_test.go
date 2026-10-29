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
	"testing"
	"time"
)

// Without an RDMA device, Fetch via RDMA must fail fast and cleanly (no
// panic, no hang) and the mount must stay serviceable (TCP fallback). With
// userland present but no device, DialRDMA succeeds lazily and the failure
// surfaces per dial — that is the desired enterprise failover behavior.
// On a cluster node with a real device this test SKIPS — run the latencybench
// `peer` matrix there instead (tools/latencybench README §RDMA).
func TestRdmaUnavailableFailsCleanly(t *testing.T) {
	if hasRDMA() {
		t.Skip("RDMA device present; run the cluster matrix instead of this negative test")
	}
	tr, err := DialRDMA()
	if err != nil {
		// Build without the rdma tag (or no userland): unsupported is fine.
		var _ = tr
		return
	}
	rc := NewRingClient(time.Second, 31, "self-test")
	rc.SetRDMA(tr)
	members := []Member{{UUID: "peer", Addr: "127.0.0.1:1", RdmaAddr: "127.0.0.1:1"}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = rc.Fetch(ctx, "g", "k", members)
	if err == nil {
		t.Fatal("fetch via unavailable RDMA must fail (or the transport is lying)")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("RDMA failure must be fast, not a timeout")
	}
}

// hasRDMA reports whether any RDMA device is visible (mirrors rdmaAvailable
// but with a stronger sysfs check so cluster nodes skip this negative test).
func hasRDMA() bool {
	entries, err := osReadDir("/sys/class/infiniband")
	return err == nil && len(entries) > 0
}
