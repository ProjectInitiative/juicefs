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
	"fmt"
	"testing"
)

func threeMembers() []Member {
	return []Member{
		{UUID: "member-a", Addr: "127.0.0.1:10001"},
		{UUID: "member-b", Addr: "127.0.0.1:10002"},
		{UUID: "member-c", Addr: "127.0.0.1:10003"},
	}
}

// Uniformity: with 10k keys over 3 members each member owns 25–42%.
func TestOwnersUniformity(t *testing.T) {
	members := threeMembers()
	counts := map[string]int{}
	const keys = 10000
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("1_%d_%d", i, 4<<20)
		owner := Owners(key, members, 1)
		if len(owner) != 1 {
			t.Fatalf("no owner for %s", key)
		}
		counts[owner[0].UUID]++
	}
	for _, m := range members {
		share := float64(counts[m.UUID]) / float64(keys)
		if share < 0.25 || share > 0.42 {
			t.Fatalf("member %s owns %.1f%% of keys, outside 25–42%%", m.UUID, share*100)
		}
		t.Logf("member %s owns %.1f%%", m.UUID, share*100)
	}
}

// Owner stability: same key, same members => same owner, repeatedly.
func TestOwnersStable(t *testing.T) {
	members := threeMembers()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("stable-key-%d", i)
		want := Owners(key, members, 1)
		for j := 0; j < 5; j++ {
			got := Owners(key, members, 1)
			if got[0].UUID != want[0].UUID {
				t.Fatalf("unstable owner for %s: %s vs %s", key, got[0].UUID, want[0].UUID)
			}
		}
	}
}

// Movement: removing one of 3 members moves <40% of keys.
func TestOwnersMovementOnRemoval(t *testing.T) {
	members := threeMembers()
	shrunk := members[:2]
	moved := 0
	const keys = 10000
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("1_%d_%d", i, 4<<20)
		before := Owners(key, members, 1)[0].UUID
		after := Owners(key, shrunk, 1)[0].UUID
		if before != after {
			moved++
		}
	}
	share := float64(moved) / float64(keys)
	if share >= 0.40 {
		t.Fatalf("removing a member moved %.1f%% of keys (>=40%%)", share*100)
	}
	t.Logf("removal moved %.1f%% of keys", share*100)
}

// Order: Owners(n>1) returns members in strict score order; top-1 is Owners(1).
func TestOwnersOrder(t *testing.T) {
	members := threeMembers()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("order-key-%d", i)
		top3 := Owners(key, members, 3)
		if len(top3) != 3 {
			t.Fatalf("want 3 owners")
		}
		if top3[0].UUID != Owners(key, members, 1)[0].UUID {
			t.Fatalf("top-1 mismatch for %s", key)
		}
		// strict score order, no duplicates
		seen := map[string]bool{}
		for j := range top3 {
			if seen[top3[j].UUID] {
				t.Fatalf("duplicate member in top-n for %s", key)
			}
			seen[top3[j].UUID] = true
			if j > 0 {
				sPrev := Score(top3[j-1].UUID, key)
				sCur := Score(top3[j].UUID, key)
				if sPrev < sCur {
					t.Fatalf("score order violated for %s", key)
				}
				if sPrev == sCur && top3[j-1].UUID > top3[j].UUID {
					t.Fatalf("tie-break order violated for %s", key)
				}
			}
		}
	}
}

// Edge cases.
func TestOwnersEdge(t *testing.T) {
	if got := Owners("k", nil, 3); got != nil {
		t.Fatal("nil members should give nil")
	}
	if got := Owners("k", threeMembers(), 0); got != nil {
		t.Fatal("n=0 should give nil")
	}
	one := []Member{{UUID: "solo", Addr: "h:1"}}
	got := Owners("k", one, 5)
	if len(got) != 1 || got[0].UUID != "solo" {
		t.Fatal("n>len should clamp to len")
	}
}
