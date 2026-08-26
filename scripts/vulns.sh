#!/usr/bin/env bash
# Copyright (c) 2026 InfAI (CC SES)
#
# Implements the vulns gate as the catalog defines it, not as govulncheck's
# exit code defines it.
#
# The criterion is "no new known vulnerability with an AVAILABLE FIX", and the
# catalog's explicit non-goal is "transitive findings without a fix - those only
# make noise". `govulncheck ./...` exits non-zero for anything that reaches our
# code, fix or no fix, so the raw command is stricter than the gate. A finding
# nobody can act on would then sit there red until somebody learned to ignore
# red, which is the failure mode the whole gate model exists to prevent.
#
# So: fail on a finding that reaches our code AND names a fixed version. Report
# the rest, loudly enough to be read, and pass.
set -uo pipefail

out="$(mktemp)"
trap 'rm -f "$out"' EXIT

if ! govulncheck -format json ./... >"$out" 2>"$out.err"; then
	# A non-zero exit is normal here: it is how govulncheck reports findings.
	# A tool failure looks different - no JSON at all - and is caught below.
	:
fi

if [ ! -s "$out" ]; then
	printf 'govulncheck produced no output; the scan did not run:\n'
	cat "$out.err" 2>/dev/null
	exit 1
fi

python3 - "$out" <<'PY'
import json, sys

text = open(sys.argv[1]).read()
decoder = json.JSONDecoder()
index = 0
findings = []
while index < len(text):
    while index < len(text) and text[index] in " \n\t\r":
        index += 1
    if index >= len(text):
        break
    obj, index = decoder.raw_decode(text, index)
    if "finding" in obj:
        findings.append(obj["finding"])

# A finding whose trace names a package (not only a module) is one govulncheck
# could tie to code that is actually reached. A module-only trace means "you
# require this", which is not the same as "you call it".
def reaches_code(finding):
    return any(step.get("package") for step in finding.get("trace", []))

actionable, informational = [], []
for finding in findings:
    if not reaches_code(finding):
        continue
    (actionable if finding.get("fixed_version") else informational).append(finding)


def describe(finding):
    trace = finding.get("trace", [{}])
    module = trace[-1].get("module", "?")
    version = trace[-1].get("version", "?")
    fixed = finding.get("fixed_version") or "no fix available"
    return f'  {finding.get("osv", "?")}  {module}@{version}  ->  {fixed}'


if informational:
    print("Reaches this code, no fix published - reported, not blocking:")
    for finding in informational:
        print(describe(finding))
    print()

if actionable:
    print("Reaches this code and a fix exists - fix or bump:")
    for finding in actionable:
        print(describe(finding))
    sys.exit(1)

print(f"No fixable vulnerability reaches this code ({len(findings)} finding(s) scanned).")
PY
