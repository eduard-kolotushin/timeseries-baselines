package baselines

import (
	"context"
	"log/slog"
	"net"
	"time"
)

// membershipMode resolves SHARD_MEMBERSHIP to the source actually in use. auto
// (the default) picks the most specific configured source in order: an explicit
// SHARD_PEERS list, then SHARD_DNS, then the Postgres heartbeat table, then this
// worker alone.
func membershipMode(cfg Config, hasStore bool) string {
	switch cfg.ShardMembership {
	case "peers", "dns":
		return cfg.ShardMembership
	case "store":
		if hasStore {
			return "store"
		}
		return "self"
	}
	switch {
	case len(cfg.ShardPeers) > 0:
		return "peers"
	case cfg.ShardDNS != "":
		return "dns"
	case hasStore:
		return "store"
	default:
		return "self"
	}
}

// resolver looks up the addresses behind SHARD_DNS.
type resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// peerSource is the membership seen by one worker: SHARD_PEERS, the addresses
// behind SHARD_DNS, the Postgres heartbeat table, or this worker alone. A failed
// lookup keeps the last good set; with no last good set the worker runs
// unsharded, which costs duplicate work but never stalls a tick.
type peerSource struct {
	self     string
	mode     string
	static   []string
	dnsName  string
	store    membership
	ttl      time.Duration
	resolver resolver
	last     []string
	warned   bool
}

func newPeerSource(cfg Config, store membership) *peerSource {
	return &peerSource{
		self:     ShardID(cfg.ShardID),
		mode:     membershipMode(cfg, store != nil),
		static:   cfg.ShardPeers,
		dnsName:  cfg.ShardDNS,
		store:    store,
		ttl:      cfg.WorkerTTL,
		resolver: net.DefaultResolver,
	}
}

// peers returns the current peer set, always including self.
func (s *peerSource) peers(ctx context.Context) []string {
	switch s.mode {
	case "peers":
		return normalizePeers(s.static, s.self)
	case "dns":
		if s.dnsName == "" || s.resolver == nil {
			return normalizePeers(s.static, s.self)
		}
		addrs, err := s.resolver.LookupHost(ctx, s.dnsName)
		return s.keep(addrs, err, "name", s.dnsName)
	case "store":
		if s.store == nil {
			return normalizePeers(nil, s.self)
		}
		ids, err := s.store.Peers(ctx, s.ttl)
		if err != nil {
			return s.keep(nil, err, "source", "store")
		}
		// An empty answer is a valid one and must not restore the previous set.
		// Peers is read before this tick writes its own heartbeat, so with a TTL
		// close to INTERVAL a worker's own row has already aged out; a departed
		// peer's row is supposed to disappear. Falling back to the last set here
		// keeps a dead worker in the view and strands the share it owns.
		s.warned = false
		s.last = normalizePeers(ids, s.self)
		return s.last
	default:
		return normalizePeers(nil, s.self)
	}
}

// keep applies the DNS fallback: a failed or empty lookup keeps the last good
// set, because an empty A-record answer there is a resolution problem rather
// than "the fleet is empty" (the headless Service always lists this worker).
func (s *peerSource) keep(ids []string, err error, kv ...any) []string {
	if err == nil && len(ids) > 0 {
		s.warned = false
		s.last = normalizePeers(ids, s.self)
		return s.last
	}
	if len(s.last) > 0 {
		if !s.warned {
			slog.Warn("peer lookup failed, keeping last peer set", append(kv, "err", err)...)
			s.warned = true
		}
		return s.last
	}
	if !s.warned {
		slog.Warn("peer lookup failed, running unsharded", append(kv, "err", err)...)
		s.warned = true
	}
	return normalizePeers(nil, s.self)
}
