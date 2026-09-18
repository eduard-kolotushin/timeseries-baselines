package baselines

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func corpus(n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("metric_%03d", i)
	}
	return out
}

func TestOwnsPartitionsEveryHashOnce(t *testing.T) {
	t.Parallel()
	hashes := corpus(256)
	for n := 1; n <= 6; n++ {
		peers := make([]string, n)
		for i := range n {
			peers[i] = fmt.Sprintf("worker-%d", i)
		}
		for _, h := range hashes {
			if Owner(h, peers) == "" {
				t.Fatalf("%d peers: hash %s has no owner", n, h)
			}
		}
		claimed := make(map[string]int, len(hashes))
		owned := make([]int, n)
		for i, self := range peers {
			for _, h := range hashes {
				if Owns(h, self, peers) {
					claimed[h]++
					owned[i]++
				}
			}
		}
		for _, h := range hashes {
			if claimed[h] != 1 {
				t.Fatalf("%d peers: hash %s claimed by %d workers", n, h, claimed[h])
			}
		}
		for i, c := range owned {
			if c == 0 {
				t.Fatalf("%d peers: worker %d owns nothing", n, i)
			}
		}
	}
}

func TestOwnsSinglePeerOwnsEverything(t *testing.T) {
	t.Parallel()
	for _, peers := range [][]string{nil, {"only"}} {
		for _, h := range corpus(32) {
			if !Owns(h, "only", peers) {
				t.Fatalf("peers %v: hash %s not owned by the single worker", peers, h)
			}
		}
	}
}

func TestOwnsKeepsMostHashesWhenPeerAdded(t *testing.T) {
	t.Parallel()
	hashes := corpus(1024)
	before := []string{"worker-0", "worker-1", "worker-2", "worker-3"}
	after := append(append([]string{}, before...), "worker-4")

	moved := 0
	for _, h := range hashes {
		if Owner(h, before) != Owner(h, after) {
			moved++
		}
	}
	// Rendezvous hashing moves about 1/5 of the hashes onto the new peer; a
	// modulo split would move nearly all of them.
	if moved == 0 || moved > len(hashes)/3 {
		t.Fatalf("moved %d of %d hashes when adding a peer", moved, len(hashes))
	}
}

func TestOwnsIgnoresPeerOrder(t *testing.T) {
	t.Parallel()
	// A worker's view arrives sorted, but a bug in the score tie-break would
	// make the answer depend on iteration order instead of the names.
	hashes := corpus(256)
	peers := []string{"a", "b", "c", "d"}
	permuted := []string{"d", "a", "c", "b"}
	for _, h := range hashes {
		if got, want := Owner(h, permuted), Owner(h, peers); got != want {
			t.Fatalf("hash %s: owner %q with permuted peers, %q in the original order", h, got, want)
		}
	}
}

func TestOwnsSpreadsHashesAcrossSimilarPeers(t *testing.T) {
	t.Parallel()
	// Regression: FNV-1a alone is linear in the byte where two peer names
	// differ, so peers sharing a prefix used to send nearly every hash to the
	// same worker and one peer change moved half the table.
	hashes := corpus(1024)
	peers := []string{"worker-0", "worker-1", "worker-2", "worker-3", "worker-4"}
	counts := make(map[string]int, len(peers))
	for _, h := range hashes {
		counts[Owner(h, peers)]++
	}
	mean := len(hashes) / len(peers)
	for _, p := range peers {
		if counts[p] < mean/2 || counts[p] > mean*2 {
			t.Fatalf("peer %s owns %d of %d hashes (want about %d): %v", p, counts[p], len(hashes), mean, counts)
		}
	}
}

func TestNormalizePeers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		peers []string
		self  string
		want  []string
	}{
		{name: "keeps self", peers: nil, self: "10.0.0.1", want: []string{"10.0.0.1"}},
		{name: "adds self", peers: []string{"b"}, self: "a", want: []string{"a", "b"}},
		{name: "trims and sorts", peers: []string{" b ", "a"}, self: "c", want: []string{"a", "b", "c"}},
		{name: "drops duplicates", peers: []string{"a", "a"}, self: "a", want: []string{"a"}},
		{name: "drops blanks", peers: []string{"", " "}, self: "", want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizePeers(tc.peers, tc.self)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v want %v", got, tc.want)
				}
			}
		})
	}
}

func TestShardIDPrefersExplicit(t *testing.T) {
	t.Parallel()
	if got := ShardID("  10.1.2.3 "); got != "10.1.2.3" {
		t.Fatalf("got %q", got)
	}
	if got := ShardID(""); got == "" {
		t.Fatal("automatic shard id is empty")
	}
}

type fakeResolver struct {
	addrs []string
	err   error
}

func (f *fakeResolver) LookupHost(context.Context, string) ([]string, error) {
	return f.addrs, f.err
}

func TestPeerSourceModes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		cfg      Config
		resolver *fakeResolver
		want     []string
	}{
		{
			name: "single worker without discovery",
			cfg:  Config{ShardID: "self"},
			want: []string{"self"},
		},
		{
			name: "static peers include self",
			cfg:  Config{ShardID: "b", ShardPeers: []string{"a", "c"}},
			want: []string{"a", "b", "c"},
		},
		{
			name:     "static peers win over the resolver",
			cfg:      Config{ShardID: "b", ShardPeers: []string{"a"}},
			resolver: &fakeResolver{addrs: []string{"9.9.9.9"}},
			want:     []string{"a", "b"},
		},
		{
			name:     "dns peers",
			cfg:      Config{ShardID: "10.0.0.1", ShardDNS: "baselines"},
			resolver: &fakeResolver{addrs: []string{"10.0.0.2", "10.0.0.1"}},
			want:     []string{"10.0.0.1", "10.0.0.2"},
		},
		{
			name:     "dns answer that omits self still includes it",
			cfg:      Config{ShardID: "10.0.0.9", ShardDNS: "baselines"},
			resolver: &fakeResolver{addrs: []string{"10.0.0.2", "10.0.0.3"}},
			want:     []string{"10.0.0.2", "10.0.0.3", "10.0.0.9"},
		},
		{
			name:     "dns failure runs unsharded",
			cfg:      Config{ShardID: "10.0.0.1", ShardDNS: "baselines"},
			resolver: &fakeResolver{err: errors.New("no such host")},
			want:     []string{"10.0.0.1"},
		},
		{
			name:     "empty dns answer runs unsharded",
			cfg:      Config{ShardID: "10.0.0.1", ShardDNS: "baselines"},
			resolver: &fakeResolver{},
			want:     []string{"10.0.0.1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newPeerSource(tc.cfg)
			if tc.resolver != nil {
				src.resolver = tc.resolver
			}
			got := src.peers(context.Background())
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v want %v", got, tc.want)
				}
			}
		})
	}
}

func TestPeerSourceKeepsLastGoodPeers(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{addrs: []string{"10.0.0.2"}}
	src := newPeerSource(Config{ShardID: "10.0.0.1", ShardDNS: "baselines"})
	src.resolver = res

	first := src.peers(context.Background())
	if len(first) != 2 {
		t.Fatalf("resolved peers %v want 2", first)
	}
	res.err = errors.New("dns down")
	second := src.peers(context.Background())
	if len(second) != 2 {
		t.Fatalf("peers after lookup failure %v want the last good set", second)
	}
}
