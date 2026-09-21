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

import "github.com/twmb/murmur3"

// Score computes the rendezvous weight of memberID for key. Deterministic
// across all members (no per-node randomness): every member computes the
// same score for the same (memberID, key) pair, so placement needs no
// shared state. Ties (astronomically unlikely with 64-bit scores) are
// broken in Owners by UUID string.
func Score(memberID, key string) uint64 {
	return murmur3.StringSum64(memberID + "\x00" + key)
}

// Owners returns the top-n members for key by rendezvous score, highest
// first. One pass over members, no maps. Ties are broken by lexicographically
// smaller UUID. members must not contain duplicate UUIDs (the registry
// guarantees uniqueness). Returns nil when members is empty or n <= 0.
func Owners(key string, members []Member, n int) []Member {
	if n <= 0 || len(members) == 0 {
		return nil
	}
	if n > len(members) {
		n = len(members)
	}
	// top holds the best-n so far, sorted descending by (score asc-uuid).
	type scored struct {
		m     Member
		score uint64
	}
	top := make([]scored, 0, n)
	for i := range members {
		s := scored{m: members[i], score: Score(members[i].UUID, key)}
		// insertion position: first entry that s outranks
		pos := len(top)
		for j := 0; j < len(top); j++ {
			if top[j].score < s.score ||
				(top[j].score == s.score && top[j].m.UUID > s.m.UUID) {
				pos = j
				break
			}
		}
		if pos == n {
			continue // outranks nothing in the current top-n
		}
		if len(top) < n {
			top = append(top, scored{})
		}
		copy(top[pos+1:], top[pos:])
		top[pos] = s
	}
	out := make([]Member, len(top))
	for i := range top {
		out[i] = top[i].m
	}
	return out
}
