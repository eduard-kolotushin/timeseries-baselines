#!/bin/sh
# Classify every module in this repo's Go build list, so a security reviewer can see per module
# whether the version a build resolves to is a tagged release. Network: one `go list -m -versions`
# per module. Writes markdown; `make deps-report` puts it in .gate-cache/.
set -eu
out="${1:-.gate-cache/deps-report.md}"
mkdir -p "$(dirname "$out")"
{
  printf '# Go module inventory\n\n'
  printf 'Repo: %s @ %s\n\n' "$(git remote get-url origin 2>/dev/null || echo '(no remote)')" "$(git rev-parse --short HEAD)"
  printf '| module | version | kind | upstream tagged releases |\n'
  printf '| --- | --- | --- | --- |\n'
  go list -m all | tail -n +2 \
    | awk '{k="tag"; if ($2 ~ /\+incompatible$/) k="incompatible"; else if ($2 ~ /[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]*-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]$/) k="pseudo"; print $1, $2, k}' \
    | while read -r mod ver kind; do
        n=$(go list -m -versions "$mod" 2>/dev/null | awk '{print NF-1}')
        printf '| %s | %s | %s | %s |\n' "$mod" "$ver" "$kind" "${n:-?}"
      done
  printf '\n`pseudo` with 0 upstream tags is a module whose upstream publishes no tags at all and no '
  printf 'consumer can pin it to one; `incompatible` is a tagged release that predates modules '
  printf '(`v2.1.0+incompatible`). Regenerate: `make deps-report`.\n'
} > "$out"
echo "wrote $out"
