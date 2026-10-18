#!/bin/sh
# Classify every module in this repo's Go build list, so a security reviewer can see per module
# whether the version a build resolves to is a tagged release. Network: one `go list -m -versions`
# per module. Writes markdown; `make deps-report` puts it in .gate-cache/.
set -eu
out="${1:-.gate-cache/deps-report.md}"
# Written beside the destination and moved into place only on success: the report is attached to a
# security ticket, so a mid-run network failure must not leave a truncated deliverable behind.
tmp="$out.tmp"
mkdir -p "$(dirname "$out")"
{
  printf '# Go module inventory\n\n'
  printf 'Repo: %s @ %s\n\n' "$(git remote get-url origin 2>/dev/null || echo '(no remote)')" "$(git rev-parse --short HEAD)"
  printf '| module | version | kind | upstream tagged releases |\n'
  printf '| --- | --- | --- | --- |\n'
  go list -m all | tail -n +2 \
    | awk '{k="tag"; if ($2 ~ /\+incompatible$/) k="incompatible"; else if ($2 ~ /[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]*-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]$/) k="pseudo"; print $1, $2, k}' \
    | while read -r mod ver kind; do
        if ! list=$(go list -m -versions "$mod" 2>/dev/null); then
          printf '| %s | %s | %s | lookup failed |\n' "$mod" "$ver" "$kind"
          continue
        fi
        n=$(printf '%s' "$list" | awk '{print NF-1}')
        printf '| %s | %s | %s | %s |\n' "$mod" "$ver" "$kind" "$n"
      done
  printf '\n`pseudo` with 0 upstream tags is a module whose upstream publishes no tags at all and no '
  printf 'consumer can pin it to one; `incompatible` is a major version whose module path has no /vN '
  printf 'suffix — a pre-modules tag (`v2.1.0+incompatible`) or a modern major that never declared one '
  printf '(`v25.12.19+incompatible`). Regenerate: `make deps-report`.\n'
} > "$tmp"
mv -f "$tmp" "$out"
echo "wrote $out"
