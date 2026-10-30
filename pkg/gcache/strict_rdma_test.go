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
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// failTransport always fails — models a broken/absent RDMA datapath.
type failTransport struct{}

func (failTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return nil, errors.New("rdma datapath down")
}
func (failTransport) Name() string { return "fail" }

// Regression (review round 2, finding A): strict mode must FAIL rather than
// silently fall back to TCP — otherwise a labeled "rsocket" result can hide
// TCP traffic. Control: without strict, the same setup succeeds via TCP.
func TestStrictRDMANoFallback(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k"] = []byte("tcp-served-data")
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	// strict + failing RDMA transport + peer advertising RDMA: must fail
	rc := NewRingClient(time.Second, 31, "self-test")
	rc.SetRDMA(failTransport{})
	rc.SetStrictRDMA(true)
	members := []Member{{UUID: "peer-1", Addr: addr, RdmaAddr: addr}}
	if _, err := rc.Fetch(context.Background(), "g", "k", members); err == nil {
		t.Fatal("strict mode must fail when RDMA is down (not fall back to TCP)")
	} else if !strings.Contains(err.Error(), "strict RDMA") {
		t.Fatalf("want a strict-RDMA error, got: %v", err)
	}

	// strict + peer WITHOUT an RDMA endpoint: must fail (no silent TCP)
	membersNoRdma := []Member{{UUID: "peer-1", Addr: addr}}
	if _, err := rc.Fetch(context.Background(), "g", "k", membersNoRdma); err == nil {
		t.Fatal("strict mode must fail when the peer advertises no RDMA endpoint")
	} else if !strings.Contains(err.Error(), "no RDMA endpoint") {
		t.Fatalf("want a no-endpoint error, got: %v", err)
	}

	// non-strict control: same failing transport falls back and succeeds
	rc2 := NewRingClient(time.Second, 31, "self-test")
	rc2.SetRDMA(failTransport{})
	r, err := rc2.Fetch(context.Background(), "g", "k", members)
	if err != nil {
		t.Fatalf("non-strict fallback broken: %v", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "tcp-served-data" {
		t.Fatalf("fallback payload mismatch: %q", b)
	}
}

// Regression (mixed-version rings): a TCP-only client (no RDMA transport)
// must not panic when a member advertises an RdmaAddr — it just uses TCP.
func TestTCPOnlyClientIgnoresRdmaAddr(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k"] = []byte("plain-tcp")
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test") // no SetRDMA
	members := []Member{{UUID: "peer-1", Addr: addr, RdmaAddr: addr}}
	r, err := rc.Fetch(context.Background(), "g", "k", members)
	if err != nil {
		t.Fatalf("tcp-only client with RdmaAddr peer: %v", err)
	}
	defer r.Close()
	if b, err := io.ReadAll(r); err != nil || string(b) != "plain-tcp" {
		t.Fatalf("payload via tcp: %q %v", b, err)
	}
}
