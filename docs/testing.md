# Testing

What is covered where, and what the integration suite is actually for.

## Scope

Holds for the test layout of this repository. The unit tests run without a network; the
suite in `pkg/tests` needs Docker and is behind a build tag, so the default command
neither builds nor runs it.

**Not this if**: a test in `pkg/tests` fails on a container that cannot be reached
rather than on an assertion. That is the local Docker networking, not this repository —
check whether containers are reachable at all before reading the failure as a finding.

**Breadth**: `allgemein` for the layout. The beliefs the integration suite asserts are
`einzelfall` per run: each one is a statement about a foreign service at the version it
was run against.

## Without a network

```
go test ./...
go test -race ./...
```

Every package is tested without a network: the external clients sit behind interfaces
and the HTTP ones behind `httptest`. The heuristic in `pkg/structure` is pure and
carries the bulk of the cases.

What the unit tests are expected to pin, from the inside out:

1. **The heuristic.** A three-level sub-metering chain is recognised as a tree; a gas
   meter never becomes the child of an electricity meter; a device without a reading
   lands at the root; two carriers in one graph do not interfere. **Every generated
   graph runs through the model's own validation** — that is the assurance the store
   would otherwise return as a 400.
2. **The pass.** Generated graph, user moves a device, a new device arrives: the moved
   one stays where the user put it, the new one is added, `system_changed` sits on the
   new edge and not on the old ones. Plus the rename case, in both directions.
3. **The Kafka consumers, against real message bytes.** A `PUT`, a `DELETE` and a
   `RIGHTS` in the wire format the producers actually write. What is checked is only
   that the right id falls out as dirty — that is all the service reads. Plus two
   properties of the hand-off: the handler returns without waiting for a pass, and ten
   messages about one group produce one pass, not ten.
4. **Restart.** After a pass, reconcile again: nothing may change. That is the proof
   that doing without a database holds — all provenance is on the graph.

## Against the real platform

```
go test -tags integration -count=1 -timeout 900s ./pkg/tests/
```

It starts MongoDB and Kafka as containers and the two services in process, seeds a group
with three meters, and drives the same full pass every trigger source ends in: the graph
is created, a device is withdrawn, two further passes have to write nothing, and an edit
made through the store has to survive a pass. Keycloak and the timescale wrapper stay
faked — the realm nobody has provisioned buys nothing here, and the readings arithmetic
is covered by `pkg/consumption`.

**The point is not the logic, which the unit tests cover, but the beliefs about the
foreign APIs that a fake can only repeat back**: what an attributes filter matches, what
a graph write does to permissions, whether a short page is the last one. Each is
asserted on its own and named in the failure.

Two of them are known to be wrong and are worked around rather than fixed — the store
ANDs a multi-key attributes filter, and the client's `attributes_json` is double-escaped
into a 400. Both are stated in [graph-contract.md](graph-contract.md). Written as
assertions rather than as comments so that a fix upstream shows up as a refuted
assumption: the suite says `CONFIRMED` or `REFUTED` per belief, and a `REFUTED` is the
signal that a workaround can go.

Around ten seconds with the images pulled, a few minutes without.

## Quality gates

`.claude/gates.env` holds the per-gate modes and the reasons for every gate that is not
enforcing. `.github/workflows/gates.yml` is what makes them binding on a pull request; a
local run only shortens the loop.
