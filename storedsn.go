package baselines

import (
	"net/url"
	"strings"
)

// storeDSN builds the Postgres DSN from BASELINE_STORE_URL, or from the
// BASELINE_STORE_* fields, falling back per field to the plugin's
// FORECAST_STORE_* names: a VM that already has the plugin's snapshot store
// configured needs no second set of variables. No URL and no host means no
// store, which keeps the worker stateless (per-tick fit, static membership).
func storeDSN(getenv func(string) string) string {
	if u := storeEnv(getenv, "URL"); u != "" {
		return u
	}
	host := storeEnv(getenv, "HOST")
	if host == "" {
		return ""
	}
	port := storeEnv(getenv, "PORT")
	if port == "" {
		port = "5432"
	}
	database := storeEnv(getenv, "DATABASE")
	if database == "" {
		database = "overlay"
	}
	user := storeEnv(getenv, "USER")
	if user == "" {
		user = "overlay"
	}
	sslmode := storeEnv(getenv, "SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}
	u := &url.URL{
		Scheme: "postgres",
		Host:   host + ":" + port,
		Path:   "/" + database,
	}
	if user != "" {
		if pass := storeEnv(getenv, "PASSWORD"); pass != "" {
			u.User = url.UserPassword(user, pass)
		} else {
			u.User = url.User(user)
		}
	}
	q := u.Query()
	q.Set("sslmode", sslmode)
	u.RawQuery = q.Encode()
	return u.String()
}

// storeEnv reads one store field, preferring the worker's own name.
func storeEnv(getenv func(string) string, field string) string {
	for _, key := range []string{"BASELINE_STORE_" + field, "FORECAST_STORE_" + field} {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
	}
	return ""
}
