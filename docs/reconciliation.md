# Reconciliation: what a pass does

One pass per group. Every trigger source ends here, and the pass is idempotent — a
backlog after a restart and a live event take the same code path.

## Scope

Holds for a pass over a group that already has a graph, and for the creation of the
first one. Assumes the provenance attributes described in
[graph-contract.md](graph-contract.md); a graph without them is not this service's and
is not touched.

**Not this if**: the initial structure is what is in question — that is
[structure-heuristic.md](structure-heuristic.md). And not this if devices seem to have
left a group *en masse*: that is the rename case below, and reading it as a removal is
destructive.

**Breadth**: `allgemein` for the pass itself. The rename safeguard rests on one
observation about how group permissions are keyed — `einzelfall`, so check that it still
holds before relying on it for anything beyond the safeguard.

## No graph yet

The full heuristic runs, the tree is validated locally, the graph is written, and the
permissions are set. See [tokens-and-permissions.md](tokens-and-permissions.md).

**Only for a group that owns at least one device.** A graph holding nothing but its
root carries no information, and a realm has far more groups than groups that own a
meter — departments, roles, the tree above a site. One graph per group would fill the
graph view with empty entries a user has to sort through. Nothing is lost by waiting:
a device joining the group triggers a pass of its own, and the safety-net pass finds it
in any case.

This is a rule for creation only. An existing graph whose last device left keeps its
root and stays shared — see below.

## Graph exists

- **New device** goes to the parent the same capacity test yields, otherwise to the
  root. `system_changed` on the new edge, not on the old ones.
- **Device left the group** — the node is deleted. The model reroutes the incoming
  edges itself and marks them `system_changed`. A graph that runs empty this way is
  **not** deleted: it may carry a user's structure, and the harsher of the two actions
  is not the one taken here, the same reasoning as for a graph whose group is gone.
- **Group renamed** — the root's `name` attribute is rewritten, but **only** while it
  still matches `graph-provider/name`. If a user renamed the unit, their name stays;
  `graph-provider/name` is updated anyway, so the next group rename does not run
  against it again.
- **Existing edges are never re-parented**, even when newer readings suggest a
  different structure.
- **Group deleted** — the graph is not deleted. Its group attribute and the group's
  share are removed, and nothing else changes.

## The rename safeguard

A renamed Keycloak group loses the permissions granted to it, platform-wide: the path
is the key on the permissions side and nothing tells it about the rename. So in the
pass after a rename, every device of that group looks as if it had left.

**On a pass where a group's path has changed, no device is removed** — however many now
look absent. Additions are still safe. Emptying a graph on that reading would destroy a
structure a user built, and it would be wrong: the devices are where they were, only
the name changed.

This is not fixable from here, and the service does not try. What it does is refuse to
draw a conclusion from it.

## Idempotence against its own events

Every write produces a command the service itself consumes: setting permissions emits a
`RIGHTS` command, writing a graph emits a content command on the same topic. Both
writes are therefore preceded by a read and skipped when target and actual state
already agree. The echo then costs exactly one further pass that finds nothing and
writes nothing; without the check the loop never terminates.

This is the property the restart test in [testing.md](testing.md) pins: after a pass,
restart and reconcile again, and nothing may change.

## Where the graph events come from

The graphs topic is registered with a Kafka target, and graph content commands are
published, **in the device-repository** — not here. That is where they belong: setting a
topic replaces its configuration wholesale, and the device-repository rewrites its
version on every start, so a foreign writer would produce a ping-pong whose outcome
depends on startup order.

The topic carries permission commands (`RIGHTS`) and content commands (`PUT`,
`DELETE`); both name the id at the top level, which is all this service reads.

A `DELETE` is the one event whose group cannot be resolved: the group is written on the
graph, and the graph is gone. There is no local record to look it up in, by design, so
a deletion triggers a full pass instead — cheap enough, since graphs are deleted rarely
and a company without a graph should not wait out the safety-net interval.

Should those events not be published — an older store — the service still works: it
would find the same facts on the next full pass. Only the latency changes.
