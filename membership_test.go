package baselines

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeResolver struct {
	addrs []string
	err   error
}

func (f *fakeResolver) LookupHost(context.Context, string) ([]string, error) {
	return f.addrs, f.err
}

// fakeMembership is the heartbeat table seen by one worker.
type fakeMembership struct {
	ids []string
	err error
}

func (f *fakeMembership) Heartbeat(context.Context, string, int, int) error { return nil }

func (f *fakeMembership) Peers(context.Context, time.Duration) ([]string, error) {
	return f.ids, f.err
}

func TestMembershipMode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		cfg      Config
		hasStore bool
		want     string
	}{
		{name: "auto without discovery", cfg: Config{ShardID: "self"}, want: "self"},
		{name: "auto prefers static peers", cfg: Config{ShardPeers: []string{"a"}}, hasStore: true, want: "peers"},
		{name: "auto prefers dns over the store", cfg: Config{ShardDNS: "baselines"}, hasStore: true, want: "dns"},
		{name: "auto falls back to the store", hasStore: true, want: "store"},
		{name: "auto without a store", hasStore: false, want: "self"},
		{name: "explicit peers wins over the store", cfg: Config{ShardMembership: "peers", ShardDNS: "baselines"}, hasStore: true, want: "peers"},
		{name: "explicit dns wins over the store", cfg: Config{ShardMembership: "dns", ShardDNS: "baselines"}, hasStore: true, want: "dns"},
		{name: "explicit store", cfg: Config{ShardMembership: "store"}, hasStore: true, want: "store"},
		{name: "explicit store without one", cfg: Config{ShardMembership: "store"}, hasStore: false, want: "self"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := membershipMode(tc.cfg, tc.hasStore); got != tc.want {
				t.Fatalf("membershipMode(%+v, %v) = %q, want %q", tc.cfg, tc.hasStore, got, tc.want)
			}
		})
	}
}

func TestPeerSourceModes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		cfg      Config
		resolver *fakeResolver
		store    *fakeMembership
		want     []string
		wantMode string
	}{
		{
			name:     "single worker without discovery",
			cfg:      Config{ShardID: "self"},
			want:     []string{"self"},
			wantMode: "self",
		},
		{
			name:     "static peers include self",
			cfg:      Config{ShardID: "b", ShardPeers: []string{"a", "c"}},
			want:     []string{"a", "b", "c"},
			wantMode: "peers",
		},
		{
			name:     "static peers win over the resolver",
			cfg:      Config{ShardID: "b", ShardPeers: []string{"a"}},
			resolver: &fakeResolver{addrs: []string{"9.9.9.9"}},
			want:     []string{"a", "b"},
			wantMode: "peers",
		},
		{
			name:     "dns peers",
			cfg:      Config{ShardID: "10.0.0.1", ShardDNS: "baselines"},
			resolver: &fakeResolver{addrs: []string{"10.0.0.2", "10.0.0.1"}},
			want:     []string{"10.0.0.1", "10.0.0.2"},
			wantMode: "dns",
		},
		{
			name:     "dns answer that omits self still includes it",
			cfg:      Config{ShardID: "10.0.0.9", ShardDNS: "baselines"},
			resolver: &fakeResolver{addrs: []string{"10.0.0.2", "10.0.0.3"}},
			want:     []string{"10.0.0.2", "10.0.0.3", "10.0.0.9"},
			wantMode: "dns",
		},
		{
			name:     "dns failure runs unsharded",
			cfg:      Config{ShardID: "10.0.0.1", ShardDNS: "baselines"},
			resolver: &fakeResolver{err: errors.New("no such host")},
			want:     []string{"10.0.0.1"},
			wantMode: "dns",
		},
		{
			name:     "empty dns answer runs unsharded",
			cfg:      Config{ShardID: "10.0.0.1", ShardDNS: "baselines"},
			resolver: &fakeResolver{},
			want:     []string{"10.0.0.1"},
			wantMode: "dns",
		},
		{
			name:     "store heartbeat peers",
			cfg:      Config{ShardID: "w0", ShardMembership: "store"},
			store:    &fakeMembership{ids: []string{"w1", "w0"}},
			want:     []string{"w0", "w1"},
			wantMode: "store",
		},
		{
			name:     "store answer that omits self still includes it",
			cfg:      Config{ShardID: "w9", ShardMembership: "store"},
			store:    &fakeMembership{ids: []string{"w1", "w2"}},
			want:     []string{"w1", "w2", "w9"},
			wantMode: "store",
		},
		{
			name:     "store failure runs unsharded",
			cfg:      Config{ShardID: "w0", ShardMembership: "store"},
			store:    &fakeMembership{err: errors.New("store is down")},
			want:     []string{"w0"},
			wantMode: "store",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store membership
			if tc.store != nil {
				store = tc.store
			}
			src := newPeerSource(tc.cfg, store)
			if tc.resolver != nil {
				src.resolver = tc.resolver
			}
			if src.mode != tc.wantMode {
				t.Fatalf("mode %q, want %q", src.mode, tc.wantMode)
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
	// A dying heartbeat must not silently collapse the fleet to one worker: the
	// surviving worker would publish every hash and duplicate the other's points.
	res := &fakeResolver{addrs: []string{"10.0.0.2"}}
	src := newPeerSource(Config{ShardID: "10.0.0.1", ShardDNS: "baselines"}, nil)
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

	store := &fakeMembership{ids: []string{"w1"}}
	src = newPeerSource(Config{ShardID: "w0", ShardMembership: "store"}, store)
	if got := src.peers(context.Background()); len(got) != 2 {
		t.Fatalf("store peers %v want 2", got)
	}
	store.err = errors.New("store is down")
	if got := src.peers(context.Background()); len(got) != 2 {
		t.Fatalf("peers after a heartbeat failure %v want the last good set", got)
	}
}

// Regression: the store path used to share the DNS fallback, which treats an
// empty answer as a failed lookup. Peers is read before the tick writes its own
// heartbeat, so with a TTL at or below INTERVAL every row can already have aged
// out; restoring the previous set then kept a departed worker in the view and
// stranded the share it owned instead of failing over.
func TestPeerSourceStoreDropsDepartedPeers(t *testing.T) {
	t.Parallel()
	store := &fakeMembership{ids: []string{"w0", "w1"}}
	src := newPeerSource(Config{ShardID: "w0", ShardMembership: "store"}, store)
	if got := src.peers(context.Background()); len(got) != 2 {
		t.Fatalf("two live workers resolved to %v, want both", got)
	}

	store.ids = []string{"w0"}
	if got := src.peers(context.Background()); len(got) != 1 || got[0] != "w0" {
		t.Fatalf("after w1 left, peers %v want this worker alone", got)
	}

	// Nobody live at all: still this worker, owning everything, rather than a
	// resurrected dead peer list.
	store.ids = nil
	if got := src.peers(context.Background()); len(got) != 1 || got[0] != "w0" {
		t.Fatalf("with no heartbeat inside WORKER_TTL, peers %v want this worker alone", got)
	}
}
