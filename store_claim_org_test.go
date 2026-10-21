package baselines

import (
	"context"
	"testing"
	"time"
)

// A baseline row is fleet-wide at org 0. A same-scope row under another org is
// not this worker's to claim: BaselineKeys and Retire are org-0 scoped, so a
// claimed org-7 row would be retrained forever while its snapshot could never be
// retired. The claim must select only the fleet-wide org.
func TestPostgresClaimStaysInTheFleetWideOrg(t *testing.T) {
	s, ctx := openTestStore(t)
	ensurePluginRetrainTable(t, s)

	// Park every due row the shared test database already carries, so the claim
	// set is exactly what this test seeds.
	if _, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain SET next_run_at = now() + interval '1 day'
WHERE scope = 'baseline' AND next_run_at <= now()
`); err != nil {
		t.Fatal(err)
	}

	const (
		fleetKey = "claim-org-zero"
		otherKey = "claim-org-seven"
	)
	seed := func(org int64, key string) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
INSERT INTO forecast.retrain (scope, org_id, key, cron, timezone, enabled, next_run_at)
VALUES ('baseline', $1, $2, '0 3 * * *', 'UTC', true, now())
ON CONFLICT (scope, org_id, key) DO UPDATE SET
  next_run_at = now(), claimed_by = NULL, claimed_until = NULL
`, org, key); err != nil {
			t.Fatal(err)
		}
	}
	seed(0, fleetKey)
	seed(7, otherKey)
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(),
			`DELETE FROM forecast.retrain WHERE scope = 'baseline' AND key = ANY($1)`,
			[]string{fleetKey, otherKey})
	})

	claims, err := s.Claim(ctx, "claim-org-test", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	claimed := make(map[string]retrainClaim, len(claims))
	for _, c := range claims {
		if c.OrgID != 0 {
			t.Fatalf("claimed %q at org %d, want only the fleet-wide org 0", c.Key, c.OrgID)
		}
		claimed[c.Key] = c
	}
	if _, ok := claimed[fleetKey]; !ok {
		t.Fatalf("claim set %v does not include the due fleet-wide row", claimed)
	}
	if _, ok := claimed[otherKey]; ok {
		t.Fatalf("claimed another org's baseline row %q", otherKey)
	}
	var otherClaimed bool
	if err := s.pool.QueryRow(ctx, `
SELECT claimed_by IS NOT NULL FROM forecast.retrain
WHERE scope = 'baseline' AND org_id = 7 AND key = $1
`, otherKey).Scan(&otherClaimed); err != nil {
		t.Fatal(err)
	}
	if otherClaimed {
		t.Fatal("another org's row was leased by this worker")
	}
}
