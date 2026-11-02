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

// Topology awareness for the distributed cache.
//
// The LOGICAL cache ring (rendezvous-hash ownership) is independent of the
// PHYSICAL network: ownership never changes with topology. What topology
// changes is WHICH ENDPOINT of a peer a client dials — switchless rings
// expose one isolated subnet per peer pair, so a node's RDMA address is
// per-pair, and a switched fabric may expose a single cluster-wide address.
//
// The connectivity graph classifies each (self, peer, endpoint) edge by
// PROBING it:
//
//	direct-rdma   endpoint IP is in a subnet of a local interface
//	              (point-to-point rail / same L2) — 0 extra hops
//	routed-rdma   endpoint reachable via rsockets but outside local
//	              subnets (switched fabric / IP-routed RoCE) — 1+ hops
//	unavailable   probe failed
//	tcp           no RDMA endpoint; TCP is the only path
//
// Sources of the candidate graph, in order: explicit Member.RdmaAddrs, the
// DGX rdma-fabric.json schema (MembersFromFabric), and NCCL topology
// exports (see docs/TOPOLOGY.md — an optional input, never a transport).

package gcache

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

// Endpoint classes reported by ProbePeer / the topo command.
const (
	ClassDirectRdma  = "direct-rdma"
	ClassRoutedRdma  = "routed-rdma"
	ClassUnavailable = "unavailable"
	ClassTCP         = "tcp"
)

// EndpointStatus is the probe result for one candidate endpoint.
type EndpointStatus struct {
	Endpoint string `json:"endpoint"`
	Class    string `json:"class"`
	Hops     int    `json:"hops"` // 0 direct, >=1 routed, -1 unknown
	OK       bool   `json:"ok"`   // rsockets dial succeeded
	LatencyUs int64 `json:"latency_us,omitempty"`
	Err      string `json:"error,omitempty"`
}

// localSubnets returns the IPv4 CIDRs of every non-loopback interface.
func localSubnets() []*net.IPNet {
	var out []*net.IPNet
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				out = append(out, &net.IPNet{IP: ipn.IP.To4().Mask(ipn.Mask), Mask: ipn.Mask})
			}
		}
	}
	return out
}

// classifyEndpoint labels an endpoint by comparing its IP against the local
// interface subnets. Same subnet => direct link (0 hops); anything else that
// still dials is routed (1+ hops, exact count unknown without traceroute).
func classifyEndpoint(ep string) (class string, hops int) {
	host, _, err := net.SplitHostPort(ep)
	if err != nil {
		host = ep
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		return ClassRoutedRdma, -1 // hostname endpoints: resolved at dial time
	}
	ip = ip.To4()
	for _, sn := range localSubnets() {
		if sn.Contains(ip) {
			return ClassDirectRdma, 0
		}
	}
	return ClassRoutedRdma, 1
}

// EndpointClassOf classifies an endpoint string without dialing:
// direct-rdma when its IP is in a local subnet, routed-rdma when remote,
// tcp when empty (TCP dials record no RDMA class). Hops: 0 direct, 1 routed.
func EndpointClassOf(ep string) (class string, hops int) {
	if ep == "" {
		return ClassTCP, 0
	}
	return classifyEndpoint(ep)
}

// rdmaDialBounded dials an RDMA endpoint for probing WITHOUT touching
// failover state. Requires an RDMA transport (nil => error).
func (c *RingClient) rdmaDialBounded(ep string, timeout time.Duration) (net.Conn, error) {
	c.mu.Lock()
	tr := c.rdma
	c.mu.Unlock()
	if tr == nil {
		return nil, fmt.Errorf("no RDMA transport configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return tr.Dial(ctx, ep)
}

// tcpDialBounded dials a TCP endpoint for probing.
func (c *RingClient) tcpDialBounded(addr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.tcp.Dial(ctx, addr)
}

// ProbePeer dials every RDMA endpoint the member advertises (plus TCP as a
// last-resort classification) and returns per-endpoint status. It never
// mutates client failover state.
func (c *RingClient) ProbePeer(m Member, timeout time.Duration) []EndpointStatus {
	var eps []string
	if m.RdmaAddr != "" {
		eps = append(eps, m.RdmaAddr)
	}
	eps = append(eps, m.RdmaAddrs...)

	var out []EndpointStatus
	for _, ep := range eps {
		st := EndpointStatus{Endpoint: ep}
		st.Class, st.Hops = classifyEndpoint(ep)
		start := time.Now()
		conn, err := c.rdmaDialBounded(ep, timeout)
		if err != nil {
			st.Err = err.Error()
			out = append(out, st)
			continue
		}
		_ = conn.Close()
		st.OK = true
		st.LatencyUs = time.Since(start).Microseconds()
		out = append(out, st)
	}
	// TCP reachability of the member's primary endpoint (informational only).
	if m.Addr != "" {
		st := EndpointStatus{Endpoint: m.Addr, Class: ClassTCP, Hops: -1}
		start := time.Now()
		conn, err := c.tcpDialBounded(m.Addr, timeout)
		if err != nil {
			st.Err = err.Error()
		} else {
			_ = conn.Close()
			st.OK = true
			st.LatencyUs = time.Since(start).Microseconds()
		}
		out = append(out, st)
	}
	return out
}

// ProbeTopology probes every member and returns the graph keyed by member
// address. Deterministic order: members sorted by Addr, endpoints in
// advertised order.
func (c *RingClient) ProbeTopology(members []Member, timeout time.Duration) map[string][]EndpointStatus {
	sort.Slice(members, func(i, j int) bool { return members[i].Addr < members[j].Addr })
	out := make(map[string][]EndpointStatus, len(members))
	for _, m := range members {
		out[m.Addr] = c.ProbePeer(m, timeout)
	}
	return out
}

// FormatTopology renders the graph as an aligned report.
func FormatTopology(graph map[string][]EndpointStatus) string {
	var b strings.Builder
	addrs := make([]string, 0, len(graph))
	for a := range graph {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	for _, a := range addrs {
		fmt.Fprintf(&b, "peer %s:\n", a)
		for _, st := range graph[a] {
			state := "ok"
			if !st.OK {
				state = "FAIL: " + st.Err
			}
			fmt.Fprintf(&b, "  %-24s %-12s hops=%d %8dus  %s\n",
				st.Endpoint, st.Class, st.Hops, st.LatencyUs, state)
		}
	}
	return b.String()
}

// FabricTopology mirrors the DGX /var/lib/dgx-spark/rdma-fabric.json schema
// (schemaVersion 1): a node's point-to-point rails with per-link addresses.
type FabricTopology struct {
	NodeName string `json:"nodeName"`
	Fabric   string `json:"fabric"`
	Links    []struct {
		ID           string `json:"id"`
		Address      string `json:"address"`      // this node's rail IP (CIDR)
		Interface    string `json:"interface"`
		PeerAddress  string `json:"peerAddress"`  // peer's rail IP on this link
		PeerNode     string `json:"peerNode"`
		PeerControlAddress string `json:"peerControlAddress"`
	} `json:"links"`
}

// MembersFromFabric builds the candidate graph from a fabric inventory:
// every OTHER node becomes a Member whose TCP Addr is the peer's control
// address and whose RdmaAddrs are its per-link rail addresses. The returned
// map groups rail endpoints by peer node. rdmaPort is the peers' RDMA
// listener port (all members bind the same port).
func MembersFromFabric(data []byte, self string, rdmaPort int, tcpPort int) (selfNode string, peers []Member, err error) {
	var ft FabricTopology
	if err = json.Unmarshal(data, &ft); err != nil {
		return "", nil, err
	}
	selfNode = ft.NodeName
	if self != "" {
		selfNode = self
	}
	byPeer := map[string][]string{}
	ctrl := map[string]string{}
	for _, l := range ft.Links {
		if l.PeerNode == "" || l.PeerAddress == "" {
			continue
		}
		byPeer[l.PeerNode] = append(byPeer[l.PeerNode], net.JoinHostPort(l.PeerAddress, portStr(rdmaPort)))
		if l.PeerControlAddress != "" {
			ctrl[l.PeerNode] = l.PeerControlAddress
		}
	}
	for peer, rails := range byPeer {
		sort.Strings(rails)
		addr := ""
		if c, ok := ctrl[peer]; ok && c != "" {
			addr = net.JoinHostPort(c, portStr(tcpPort))
		}
		peers = append(peers, Member{
			UUID:       "node-" + peer,
			Addr:       addr,
			RdmaAddrs:  rails,
		})
	}
	if len(peers) == 0 {
		return selfNode, nil, fmt.Errorf("fabric %s: no links with a peer node", ft.Fabric)
	}
	return selfNode, peers, nil
}

func portStr(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
