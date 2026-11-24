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

**Also for a group that owns no device yet.** Its graph then holds only the root and is
shared like any other. Devices the group is given later are added to it as described
below. Which groups get a graph at all is narrowed with `group_include` and
`group_exclude`, see [configuration.md](configuration.md).

An existing graph whose last device left likewise keeps its root and stays shared — see
below.

## Meters: how a device is added

Both the first graph and a later pass place a device as a meter thing, see
[design-decisions.md](design-decisions.md). Before the graph is written, per device:

1. One listing of the things linked to the new devices. A device with an open link to a
   meter thing that this service owns and marked with this group's id is **adopted**: that
   is what an earlier pass left behind when it made the thing and failed before the graph
   was written. A second thing for the device would be refused anyway, since the periods
   of one device may not overlap across things. An adopted thing is shared again, the
   earlier pass may have stopped before that.
2. A device with an open link to any **other** thing cannot get a second one. It is placed
   as a device node, as before, and the pass says so at warn.
3. Otherwise the thing is created (`POST /things`, `kind: meter`, name of the device,
   owner the service user), the device linked to it (`POST /things/{id}/device-links`,
   `from` left out, which the repository reads as now) and the thing shared with the
   group. A link that fails takes its thing with it, best effort.
4. Any failure ends the group's pass before the graph is written. For the first graph
   that matters: it is the one place the whole heuristic runs, and a graph written
   without some devices would have them attached one by one later.

With writing off nothing is created or shared; the devices get a placeholder id so the
report can still show the graph.

**Which devices count as placed.** A device node, or an open link - open now or still
ahead - of any thing that has a node in the projection at now. Any kind of thing, not
only meters: the repository refuses a graph holding a thing together with a device
linked to it at the same time, so adding such a device as a node would fail on every
pass. A link that has ended does not count, and its device is placed again as a new
meter. A graph without thing nodes costs no extra request.

## Graph exists

- **New device** is placed as a meter (above), under the parent the same capacity test
  yields, otherwise under the root. `system_changed` on the new edge, not on the old
  ones. The capacity test reads a thing node through the device linked to it.
- **Device left the group** — a device node is deleted. A **meter is not**: the thing
  and its node stay. The open link of the device on the group's own meter is ended at now
  (`POST /things/{id}/device-links/{link}/end`, no `until_values`), but only after the
  device was missing in two consecutive passes, counted in memory per link; a pass in
  which it is back resets the count, a rename pass does too, a restart starts over. Links
  on foreign things are never touched. See [known-limitations.md](known-limitations.md).
- **Group renamed** — the sharing of the meters this service made follows the graph: the
  new path is granted and the old one withdrawn, thing by thing. Only on a rename, not on
  every pass; see known-limitations. The root's `name` attribute is rewritten, but **only** while it
  still matches `graph-provider/name`. If a user renamed the unit, their name stays;
  `graph-provider/name` is updated anyway, so the next group rename does not run
  against it again.
- **Existing edges are never re-parented**, even when newer readings suggest a
  different structure.
- **Group deleted** — the graph is not deleted. Its group attribute and the group's
  share are removed, and nothing else changes.

## Writing: history, conflicts

`ListGraphs` answers the projection at now: nodes, edges and attributes that ended or
have not started are not in it. The pass edits that and writes it with `PUT` and no `at`,
which is a correction at now; the repository keeps the versions the body does not show.
Three consequences:

- **No node changes what it stands for.** A pass only adds nodes and removes device
  nodes. Before writing an update, the node ids of the stored graph are compared with the
  edited one and a change of type or resource is refused locally.
- **A new element carries no validity**, and cannot: the models module this service is
  pinned to has no such fields.
- **409** is the repository's compare-and-set losing five times against other writers.
  It is returned like any transient failure, named in the log at warn, and the next pass
  reads the graph again and applies the same edit.

A write that **removes** something - a departed device node - is the exception: it is
`PUT /graphs/{id}?at=<now>&mode=change`. The node and its edges end now and the history
stays. As a correction the node would be deleted from the stored document, which the
repository refuses (400, naming the instant) while an ended version of an edge still points
at it. A write that only adds stays a correction without `at`; if one pass both removes and
adds, the whole write is the change, and the additions start now.

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
