#!/usr/bin/env bash
# Copyright (c) 2026 InfAI (CC SES)
#
# The coverage_delta gate, locally.
#
# The criterion from the catalog: coverage of the lines this branch CHANGED is
# not below the repository average. Not an absolute percentage - an absolute
# threshold either blocks everything or is set so low it checks nothing, while
# this one is meetable on day one and only ever rises.
#
# The gates runner ships no implementation, so under enforce it reports "not
# implemented", which is red forever. This is the implementation.
#
# With no base ref to compare against - a repository whose history has not
# started yet - there is no changed set, and the gate says so and passes. That
# is a fact about the repository, not a hole in the check.
set -uo pipefail

profile="$(mktemp)"
trap 'rm -f "$profile"' EXIT

if ! go test -coverprofile="$profile" -covermode=set ./... >/dev/null 2>&1; then
	printf 'the test run failed, so there is no coverage to judge; fix the suite first\n'
	exit 1
fi

python3 - "$profile" "${BASE_REF:-}" <<'PY'
import subprocess, sys

profile_path, base_ref = sys.argv[1], sys.argv[2]


def git(*args):
    result = subprocess.run(["git", *args], capture_output=True, text=True)
    return result.returncode, result.stdout.strip()


def module_path():
    result = subprocess.run(["go", "list", "-m"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def resolve_base():
    """The commit this branch is measured against, or None."""
    candidates = [base_ref] if base_ref else []
    candidates += ["origin/HEAD", "main", "master", "develop", "trunk"]
    for candidate in candidates:
        if not candidate:
            continue
        code, resolved = git("rev-parse", "--verify", "--quiet", candidate)
        if code == 0 and resolved:
            return candidate
    return None


def changed_lines(base):
    """Repo-relative file -> set of line numbers this branch adds or changes."""
    code, out = git("diff", "--unified=0", "--no-color", f"{base}...HEAD")
    if code != 0:
        return None
    changed, current = {}, None
    for line in out.splitlines():
        if line.startswith("+++ b/"):
            current = line[6:]
        elif line.startswith("@@") and current:
            # @@ -old,+new @@ - only the new side matters, it is what exists now.
            new = line.split("+", 1)[1].split(" ", 1)[0]
            start, _, count = new.partition(",")
            start, count = int(start), int(count or 1)
            changed.setdefault(current, set()).update(range(start, start + count))
    return changed


def blocks(path, module):
    """(file, first line, last line, statements, covered) per profile block."""
    result = []
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("mode:"):
                continue
            location, _, statements_and_count = line.partition(" ")
            statements, _, count = statements_and_count.partition(" ")
            name, _, span = location.partition(":")
            start, _, end = span.partition(",")
            first = int(start.split(".")[0])
            last = int(end.split(".")[0])
            if module and name.startswith(module + "/"):
                name = name[len(module) + 1:]
            result.append((name, first, last, int(statements), int(count) > 0))
    return result


module = module_path()
profile = blocks(profile_path, module)
total = sum(block[3] for block in profile)
covered = sum(block[3] for block in profile if block[4])
average = covered / total if total else 1.0

base = resolve_base()
if base is None:
    print(f"no base ref to compare against; repository coverage is {average:.1%}")
    print("the changed set is undefined until the history starts, so nothing to judge")
    sys.exit(0)

changed = changed_lines(base)
if changed is None:
    print(f"unable to diff against {base}")
    sys.exit(1)

changed_total = changed_covered = 0
for name, first, last, statements, is_covered in profile:
    lines = changed.get(name)
    if not lines:
        continue
    if any(line in lines for line in range(first, last + 1)):
        changed_total += statements
        if is_covered:
            changed_covered += statements

if changed_total == 0:
    print(f"no changed Go statements against {base}; repository coverage is {average:.1%}")
    sys.exit(0)

ratio = changed_covered / changed_total
print(f"changed lines {ratio:.1%} ({changed_covered}/{changed_total} statements)"
      f" vs repository average {average:.1%}, measured against {base}")

# A hair of tolerance: the two numbers are ratios over different denominators,
# and failing a change for a rounding difference would be noise.
if ratio + 0.005 < average:
    print("the code this branch changed is tested less than the repository average")
    sys.exit(1)
PY
