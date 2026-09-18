package baselines

import (
	"context"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
)

// Ownership is rendezvous ("highest random weight") hashing over the peer set:
// the peer with the highest hash of (metric_hash, peer) owns that hash. Every
// worker derives the same owner from the same peer set without a coordinator,
// and adding or removing a worker moves only the hashes that worker now owns.
//
// A worker always keeps itself in its own view, so two workers that see the
// same peers publish disjoint sets that cover every hash. Their views can
// disagree while membership changes: with SHARD_DNS the view is the live
// endpoint set, so a handover costs at most one tick and duplicates no worse
// than an extra point for the hashes that moved. A static SHARD_PEERS list must
// match the running workers, because a peer that is listed but not running
// strands its share.

const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// fnv1a continues an FNV-1a hash over s.
func fnv1a(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

// pairHash hashes "hash|peer" without allocating the joined string. The final
// mixing step matters: raw FNV-1a is linear in the differing byte, so peers
// whose names share a prefix would produce near-identical scores for the same
// hash and ownership would follow the peer name instead of the hash.
func pairHash(hash, peer string) uint64 {
	h := fnv1a(fnvOffset64, hash)
	h ^= uint64('|')
	h *= fnvPrime64
	return mix64(fnv1a(h, peer))
}

// mix64 is the MurmurHash3 fmix64 finalizer: full avalanche, no allocation.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Owner returns the peer that owns hash. peers must not be empty.
func Owner(hash string, peers []string) string {
	var (
		best      string
		bestScore uint64
		first     = true
	)
	for _, p := range peers {
		score := pairHash(hash, p)
		if first || score > bestScore || (score == bestScore && p < best) {
			best, bestScore, first = p, score, false
		}
	}
	return best
}

// Owns reports whether self owns hash. A single peer owns every hash.
func Owns(hash, self string, peers []string) bool {
	if len(peers) <= 1 {
		return true
	}
	return Owner(hash, peers) == self
}

// normalizePeers trims and sorts the peer identities and guarantees self is in
// the set: a worker that cannot see itself would publish nothing.
func normalizePeers(peers []string, self string) []string {
	out := make([]string, 0, len(peers)+1)
	seen := make(map[string]struct{}, len(peers)+1)
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for _, p := range peers {
		add(p)
	}
	add(self)
	sort.Strings(out)
	return out
}

// ShardID is this worker's identity: SHARD_ID when set, else the first
// non-loopback IP, else the hostname.
func ShardID(explicit string) string {
	if v := strings.TrimSpace(explicit); v != "" {
		return v
	}
	if ip := localIP(); ip != "" {
		return ip
	}
	if h, err := os.Hostname(); err == nil && strings.TrimSpace(h) != "" {
		return h
	}
	return "worker"
}

// localIP prefers an IPv4 address so it matches the A records behind SHARD_DNS.
func localIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	fallback := ""
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			if v4 := ip.To4(); v4 != nil {
				return v4.String()
			}
			if fallback == "" {
				fallback = ip.String()
			}
		}
	}
	return fallback
}

// resolver looks up the addresses behind SHARD_DNS.
type resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// peerSource is the membership seen by one worker: SHARD_PEERS when set, else
// the addresses behind SHARD_DNS, else this worker alone. A failed lookup keeps
// the last good set; with no last good set the worker runs unsharded, which
// costs duplicate work but never stalls a tick.
type peerSource struct {
	self     string
	static   []string
	dnsName  string
	resolver resolver
	last     []string
	warned   bool
}

func newPeerSource(cfg Config) *peerSource {
	return &peerSource{
		self:     ShardID(cfg.ShardID),
		static:   cfg.ShardPeers,
		dnsName:  cfg.ShardDNS,
		resolver: net.DefaultResolver,
	}
}

func (s *peerSource) peers(ctx context.Context) []string {
	if s.dnsName == "" || s.resolver == nil {
		return normalizePeers(s.static, s.self)
	}
	addrs, err := s.resolver.LookupHost(ctx, s.dnsName)
	if err != nil || len(addrs) == 0 {
		if len(s.last) > 0 {
			if !s.warned {
				slog.Warn("peer lookup failed, keeping last peer set", "name", s.dnsName, "err", err)
				s.warned = true
			}
			return s.last
		}
		if !s.warned {
			slog.Warn("peer lookup failed, running unsharded", "name", s.dnsName, "err", err)
			s.warned = true
		}
		return normalizePeers(nil, s.self)
	}
	s.warned = false
	s.last = normalizePeers(addrs, s.self)
	return s.last
}
