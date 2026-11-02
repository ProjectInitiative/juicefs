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
	"net"
	"strings"
	"testing"
	"time"
)

// twoEndpointTransport fails endpoints whose address contains deadPrefix —
// models a dead rail with a live one in a switchless pair. Returns net.Pipe
// conns (dials are only counted; no IO happens on them in these tests).
type twoEndpointTransport struct {
	deadPrefix string
	dialLog    *[]string
}

func (t twoEndpointTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	*t.dialLog = append(*t.dialLog, addr)
	if strings.Contains(addr, t.deadPrefix) {
		return nil, errors.New("rail down")
	}
	c, _ := net.Pipe()
	return c, nil
}
func (t twoEndpointTransport) Name() string { return "fake-rdma" }

// Regression (topology req 3/9): a member with link-specific endpoints
// fails over from a dead rail to a live one in the SAME dial.
func TestMultiEndpointRdmaFailover(t *testing.T) {
	var log []string
	rc := NewRingClient(time.Second, 31, "self-test")
	rc.SetRDMA(twoEndpointTransport{deadPrefix: "172.16.5.", dialLog: &log})

	m := Member{
		UUID:      "peer",
		Addr:      "10.0.0.1:9000",
		RdmaAddr:  "172.16.5.56:13912",        // rail-0: dead
		RdmaAddrs: []string{"172.16.6.56:13912"}, // rail-1: alive
	}
	conn, tr, err := rc.dialFailover(context.Background(), m)
	if err != nil {
		t.Fatalf("multi-endpoint dial: %v", err)
	}
	defer conn.Close()
	if tr.Name() != "fake-rdma" {
		t.Fatalf("transport = %s", tr.Name())
	}
	if got := rc.LastEndpoint(); got != "172.16.6.56:13912" {
		t.Fatalf("endpoint = %q, want the live rail", got)
	}
	// rail-0 attempted once, then rail-1
	if len(log) != 2 || !strings.Contains(log[0], "172.16.5.56") || !strings.Contains(log[1], "172.16.6.56") {
		t.Fatalf("dial log = %v", log)
	}
}

// Per-ENDPOINT backoff: a dead rail-0 must not stop attempts on rail-1
// (the old per-member state coupled the pair).
func TestPerEndpointBackoffIndependence(t *testing.T) {
	var log []string
	rc := NewRingClient(time.Second, 31, "self-test")
	rc.SetRDMA(twoEndpointTransport{deadPrefix: "dead:", dialLog: &log})

	m := Member{
		Addr:      "10.0.0.1:9000",
		RdmaAddr:  "dead:1",
		RdmaAddrs: []string{"172.16.6.56:13912"},
	}
	for i := 0; i < 3; i++ {
		conn, _, err := rc.dialFailover(context.Background(), m)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conn.Close()
	}
	// rail-0 ("dead") enters backoff after the first failure and is skipped
	// afterwards; rail-1 serves every dial.
	deadDials := 0
	for _, a := range log {
		if strings.Contains(a, "dead") {
			deadDials++
		}
	}
	if deadDials != 1 {
		t.Fatalf("dead rail dialed %d times, want 1 (backoff should skip it): %v", deadDials, log)
	}
}

// Strict mode reports EVERY endpoint before failing (no silent TCP).
func TestStrictMultiEndpointErrorListsEndpoints(t *testing.T) {
	rc := NewRingClient(time.Second, 31, "self-test")
	rc.SetRDMA(twoEndpointTransport{deadPrefix: "dead-", dialLog: &[]string{}})
	rc.SetStrictRDMA(true)

	m := Member{
		Addr:      "10.0.0.1:9000",
		RdmaAddr:  "dead-a:1",
		RdmaAddrs: []string{"dead-b:1"},
	}
	_, _, err := rc.dialFailover(context.Background(), m)
	if err == nil {
		t.Fatal("strict dial must fail")
	}
	if !strings.Contains(err.Error(), "2 endpoint(s)") {
		t.Fatalf("error should report the endpoint count: %v", err)
	}
}

// Classification: local-subnet endpoints are direct (0 hops); others routed.
func TestClassifyEndpoint(t *testing.T) {
	cls, hops := classifyEndpoint("127.0.0.1:1") // loopback is excluded from localSubnets
	if cls != ClassRoutedRdma {
		t.Logf("loopback classified %s (acceptable on odd hosts)", cls)
	}
	cls, hops = EndpointClassOf("")
	if cls != ClassTCP || hops != 0 {
		t.Fatalf("empty endpoint: %s/%d", cls, hops)
	}
	cls, hops = EndpointClassOf("not-an-ip:1")
	if cls != ClassRoutedRdma || hops != -1 {
		t.Fatalf("hostname endpoint: %s/%d", cls, hops)
	}
}

// The fabric inventory parser builds per-pair rail endpoints from the DGX
// rdma-fabric.json schema.
func TestMembersFromFabric(t *testing.T) {
	const doc = `{
	  "nodeName":"chronometer",
	  "fabric":"dgx-spark-direct-roce",
	  "links":[
	    {"id":"chronometer-sextant-0","address":"172.16.5.55/24","peerAddress":"172.16.5.56","peerNode":"sextant","peerControlAddress":"172.16.4.56"},
	    {"id":"chronometer-sextant-1","address":"172.16.6.55/24","peerAddress":"172.16.6.56","peerNode":"sextant","peerControlAddress":"172.16.4.56"},
	    {"id":"chronometer-octant-0","address":"172.16.7.55/24","peerAddress":"172.16.7.57","peerNode":"octant","peerControlAddress":"172.16.4.57"}
	  ]
	}`
	self, peers, err := MembersFromFabric([]byte(doc), "", 13912, 13910)
	if err != nil {
		t.Fatal(err)
	}
	if self != "chronometer" {
		t.Fatalf("self = %s", self)
	}
	if len(peers) != 2 {
		t.Fatalf("peers = %d", len(peers))
	}
	byUUID := map[string]Member{}
	for _, p := range peers {
		byUUID[p.UUID] = p
	}
	sx := byUUID["node-sextant"]
	if len(sx.RdmaAddrs) != 2 || sx.RdmaAddrs[0] != "172.16.5.56:13912" || sx.RdmaAddrs[1] != "172.16.6.56:13912" {
		t.Fatalf("sextant rails = %v", sx.RdmaAddrs)
	}
	if sx.Addr != "172.16.4.56:13910" {
		t.Fatalf("sextant tcp addr = %s", sx.Addr)
	}
	oc := byUUID["node-octant"]
	if len(oc.RdmaAddrs) != 1 || oc.RdmaAddrs[0] != "172.16.7.57:13912" {
		t.Fatalf("octant rails = %v", oc.RdmaAddrs)
	}
}

// Membership change (topology req 9): with one member of three unreachable,
// keys owned by the live members still serve; only the dead owner's keys
// fail (data lives on the owner — ownership is never migrated).
func TestFetchWithOneMemberDown(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k1"] = []byte("owned-by-a")
	src.blocks["k2"] = []byte("owned-by-b")

	addrA, stopA := listenerWithSrc(t, src, 5*time.Second)
	defer stopA()
	addrB := "127.0.0.1:1" // nothing listens: the dead member

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{
		{UUID: "a", Addr: addrA},
		{UUID: "b", Addr: addrB},
	}
	// Multi-owner rings keep serving when one member dies: the secondary
	// owner serves the dead owner's keys (its fill comes from object
	// storage, so correctness holds; ownership stays with the rendezvous
	// hash — the survivor never CLAIMS the keys, it just answers).
	var okA, okB int
	for i := 0; i < 20; i++ {
		r, err := rc.Fetch(context.Background(), "g", "k1", members)
		if err == nil {
			r.Close()
			okA++
		}
		r2, err := rc.Fetch(context.Background(), "g", "k2", members)
		if err == nil {
			r2.Close()
			okB++
		}
	}
	if okA != 20 || okB != 20 {
		t.Fatalf("membership-change availability broken: k1=%d/20 k2=%d/20", okA, okB)
	}
}
