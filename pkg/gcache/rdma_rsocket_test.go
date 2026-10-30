//go:build rdma

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
	"encoding/binary"
	"testing"
)

// Regression (review round 3, finding 3): the dialer once passed
// binary.BigEndian.Uint32(ip) to C and assigned it to sin_addr.s_addr. On a
// little-endian host that stores the octets byte-swapped — 10.42.1.2 became
// 2.1.42.10. The fix passes the 4 octets in network order and memcpy's them
// into s_addr; this test pins the octet contract and documents why the
// uint32 round-trip can never work.
func TestIPv4OctetsByteOrder(t *testing.T) {
	oct, err := ipv4Octets("10.42.1.2")
	if err != nil {
		t.Fatal(err)
	}
	// network order = the dotted-quad octets verbatim, regardless of host
	// endianness — exactly what memcpy into sin_addr expects.
	if oct != [4]byte{10, 42, 1, 2} {
		t.Fatalf("ipv4Octets(10.42.1.2) = %v, want [10 42 1 2]", oct)
	}

	// The historical bug: interpreting the octets as a big-endian integer and
	// storing that integer little-endian (what a plain uint32 assignment does
	// on LE hosts) reorders the address.
	be := binary.BigEndian.Uint32(oct[:])
	var swapped [4]byte
	binary.LittleEndian.PutUint32(swapped[:], be)
	if swapped == oct {
		t.Fatal("uint32 round-trip unexpectedly preserved network order")
	}
	if swapped != [4]byte{2, 1, 42, 10} {
		t.Fatalf("expected the documented byte-swap [2 1 42 10], got %v", swapped)
	}

	// non-IPv4 input is rejected, not silently mangled
	if _, err := ipv4Octets("not-an-ip"); err == nil {
		t.Fatal("expected an error for a non-IPv4 host")
	}
	if _, err := ipv4Octets("::1"); err == nil {
		t.Fatal("expected an error for an IPv6-only host (rsockets v1)")
	}
}
