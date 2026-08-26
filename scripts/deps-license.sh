#!/usr/bin/env bash
# Copyright (c) 2026 InfAI (CC SES)
#
# The deps_license gate, locally.
#
# The gates runner has no Go implementation for this and reports it as "not
# checked" - which under enforce is red, and a gate that can never be green
# teaches people to ignore red. The standards plugin's commit hook does guard
# the manifest, but this runner cannot observe that, and an obligation nobody
# can see is not one.
#
# So: classify every module in the build against an allow list of permissive
# licence texts and fail on anything that does not match. Classification reads
# the licence file that ships with the module - the authority - rather than
# guessing from the module path. No network, no new tool.
#
# An allow list rather than a deny list, deliberately. A deny list has to name
# every licence we will not take and is wrong the moment a new one appears; an
# allow list fails closed on anything it has not been taught, which is the safe
# direction and the one the standards ask for.
#
# The catalog's non-goal holds: this reads the licence files modules ship with,
# it does not scan vendored source for embedded third-party code.
set -uo pipefail

python3 - <<'PY'
import os, re, subprocess, sys

# Permitted licences, keyed by a phrase that appears in the licence text
# itself. Ordered so a more specific phrase is tried before a more general one.
PERMITTED = [
    ("Apache-2.0", "Apache License"),
    ("ISC", "Permission to use, copy, modify, and/or distribute this software"),
    ("BSD-3-Clause", "Neither the name of"),
    ("BSD-2-Clause", "Redistribution and use in source and binary forms"),
    ("MIT", "Permission is hereby granted, free of charge"),
    ("Unlicense", "This is free and unencumbered software released into the public domain"),
]

LICENCE_FILE = re.compile(r"^(LICEN[CS]E|COPYING)([.\-].*)?$", re.IGNORECASE)


def modules():
    """Every module the build actually pulls in, path -> directory on disk."""
    out = subprocess.run(
        ["go", "list", "-deps", "-f", "{{with .Module}}{{.Path}}\t{{.Dir}}{{end}}", "./..."],
        capture_output=True, text=True, check=True,
    ).stdout
    seen = {}
    for line in out.splitlines():
        path, _, directory = line.partition("\t")
        if path.strip() and directory.strip():
            seen[path] = directory
    return seen


def licence_text(directory):
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return None
    for name in names:
        if LICENCE_FILE.match(name):
            try:
                with open(os.path.join(directory, name), errors="replace") as handle:
                    return handle.read()
            except OSError:
                return None
    return None


def classify(text):
    upper = text.upper()
    for name, phrase in PERMITTED:
        if phrase.upper() in upper:
            return name
    return None


found = modules()
# The module under test needs no permission from itself.
found.pop("github.com/SENERGY-Platform/graph-provider", None)

problems = []
counts = {}
for path in sorted(found):
    text = licence_text(found[path])
    if text is None:
        problems.append(f"  {path}: no licence file in the module")
        continue
    name = classify(text)
    if name is None:
        problems.append(f"  {path}: licence text matches none of the permitted licences")
    else:
        counts[name] = counts.get(name, 0) + 1

if problems:
    print("Dependencies whose licence is not permitted or not determinable:")
    print("\n".join(problems))
    print()
    print("Only permissive licences are allowed. If one of these is in fact")
    print("permissive, teach PERMITTED in this script rather than skipping it.")
    print("See /bitnify-standards:licensing.")
    sys.exit(1)

summary = ", ".join(f"{name} {count}" for name, count in sorted(counts.items()))
print(f"{len(found)} module(s) in the build, all permitted: {summary}")
PY
