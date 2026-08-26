# Structure heuristic: readings to a tree

How the initial structure of a generated graph is derived. Runs once per group, at
graph creation; afterwards only single devices are placed by the same test.

## Scope

Holds for the heuristic in `pkg/structure` together with the carrier analysis in
`pkg/carrier` and the consumption queries in `pkg/consumption`. The measuring-function
ids below are platform constants; the tolerance and the reference window are
configuration.

**Not this if**: the question is what happens to an *existing* graph on a later pass —
existing edges are never re-parented, see [reconciliation.md](reconciliation.md). And
not this if a placement looks wrong: the heuristic is known to be wrong in four ways
that are not defects, see [known-limitations.md](known-limitations.md) before changing
anything here.

**Breadth**: `mehrfach` for the mechanics, which follow from the graph model and the
timescale wrapper's own contract. The tuning — tightest fit, 5 % tolerance, 30-day
window — is `einzelfall`: it was chosen against real sites, not derived.

## The pass

### 1. Devices of the group

Those whose group permissions include the group path with at least `read`.

### 2. Carriers and columns per device

There is no "gas meter" field. A carrier is identified by the measuring function
annotated on a device type's output content variables:

| Carrier | Measuring function |
| --- | --- |
| electricity | `urn:infai:ses:measuring-function:57dfd369-92db-462c-aca4-a767b52c972e` |
| gas | `urn:infai:ses:measuring-function:0bab7253-5e8a-4e7c-9005-39724d6a2b4f` |
| oil | `urn:infai:ses:measuring-function:a75687f5-8bca-421f-8e57-a11176c31591` |
| water | `urn:infai:ses:measuring-function:81fbff32-c4a4-4f13-96ca-cfc7a4cfa4f0` |

The column name is the JSON path from the output root down to the annotated leaf,
joined with `.`. A device whose type reads no carrier carries no energy flow; it is
attached to the root and nothing is guessed about it.

**A path is only produced when every segment can be addressed as a column.** The two
ends of this disagree, and a valid device type can sit in the gap: the device
repository accepts `*` as the name of a list's single sub-variable — that is how a list
of variable length is modelled — while the timescale wrapper validates a column name
against `[A-Za-z0-9._-]` and rejects the **whole request** when one element fails. Left
in, one such device type would cost the readings of every device batched with it, and
its group would never get a graph at all. Such a branch therefore ends in the carrier
analysis and the device becomes one that reads no carrier. Numeric indices stay: the
wrapper's character class contains digits.

**The same value annotated on two services is asked about once.** A getter service and
an event service exposing one reading is the usual pattern. Summing both would report
twice what the meter counted, and downstream that is not a wrong number but a wrong
tree: the device sorts ahead of its real parent and becomes a parent itself.

### 3. Consumption per device and carrier

Over a reference window, default 30 days. Recipe below.

### 4. Sub-metering detection, per carrier

A gas meter cannot be a sub-meter of an electricity meter, so detection runs per
carrier.

The rule the graph view computes with is that a sub-meter sits *inside* its parent's
reading rather than on top of it. The test follows from it: P can be the parent of C if
C's consumption together with the children already assigned does not exceed P's.

Descending by consumption:

- The largest device of the carrier becomes a child of the root, with remaining
  capacity equal to its consumption.
- Every further device goes to the best-fitting parent with sufficient remaining
  capacity: among all candidates with `remaining >= consumption * (1 - tolerance)`, the
  one with the **smallest** remaining capacity. The tightest container that still fits
  is the most likely direct parent. Ties are broken by the longest common name prefix.
- If nothing fits, the device becomes a child of the root.
- Tolerance is configurable, default 5 %, to absorb reading intervals and rounding.

**Devices with no reading are not placed, and where they hang depends on why.** A
device whose type reads no carrier at all — a contact, a motion sensor, a remote —
collects under the synthetic no-flow node, because a real site has many of them. A
device that *could* have reported and did not stays at the root, where it stands out as
something to look into. Neither is offered as a parent: a device with no number under a
guessed parent is a structure nobody can check.

### 5. Write the tree

Root `root`, resource type `dashboard-custom`, group name as its `name` attribute.
Device nodes carry `id == resource_id`. Every edge points child to parent with weight
100 — in a tree each node has exactly one parent, so no shares need to be estimated.
See [graph-contract.md](graph-contract.md).

### 6. Validate locally before sending

The model's own validation is cheap and separates a heuristic bug from a transport
error.

## Consumption query recipe

Meters are cumulative, so consumption over a window is the difference between its two
ends, not a sum or an average.

- Two request elements per column, `limit: 1` each, ordered ascending and descending;
  subtract locally.
- **`orderColumnIndex: 0` must be sent.** Without the column the wrapper discards
  `orderDirection` and still applies the limit, so both ends return the same row and
  every difference is zero. This is the failure that looks like a platform with no
  consumption on it.
- One column per request element; responses are matched by `requestIndex`.
- Server-side `difference-last` aggregation was measured against the raw readings at
  the window's ends and did not agree with them. It is not used.
