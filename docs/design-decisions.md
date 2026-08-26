# Design decisions and rejected alternatives

Why the service is shaped the way it is, and what was considered and dropped. Kept
because a rejected alternative is the part nobody reconstructs later — the shape that
was chosen is visible in the code, the three that were not are not.

## Scope

Holds for the decisions that are load-bearing: removing one of them changes what the
service is, not how it is written. Each names its reason, and where a decision rests on
a fact about a foreign system, that fact is named too, so the decision can be re-checked
when the system changes.

**Not this if**: you are after the mechanics rather than the reasoning — those are in
[architecture.md](architecture.md), [reconciliation.md](reconciliation.md) and
[structure-heuristic.md](structure-heuristic.md).

**Breadth**: `allgemein` for the reasoning, which follows from the design of the
systems involved. The individual observations about foreign services are marked where
they are `einzelfall`.

## A separate service, not part of the device-repository

Deriving a structure from readings needs the timescale wrapper and the Keycloak Admin
API. The device-repository has neither and should not get them: it is the store for
graphs, not their author. Adding two outbound dependencies to a storage service to give
it an opinion about content is the larger change of the two.

## No database

Everything the service holds is a cache, rebuildable from the device-repository,
permissions-v2 and Keycloak. A database would be a second copy of state that is already
authoritative elsewhere, plus a migration and a backup story for data whose loss costs
one refresh.

What that needs in exchange is somewhere to record what the service itself wrote, so a
later run can tell its own work from a user's edit. That record lives on the graph, as
attributes — provenance travels with the object it describes, so a restart changes
nothing and there is no store to keep in step. See
[graph-contract.md](graph-contract.md).

**Rejected: a local snapshot of the graphs.** It would have to be reconciled with the
repository on every pass anyway, because a user may have edited in between — so it buys
nothing and can be wrong.

## Kafka is a trigger, not a source

Nothing is derived from a message body.

Not because a replay would be incomplete: the topics are created with `retention.ms=-1`
and `cleanup.policy=compact`, so a full read does yield the latest state of every key.
An earlier version of this reasoning claimed a few days of retention and was simply
wrong about it. The reason is that a projection built here would be a second opinion
about state this service does not own. Two copies can disagree, and the one built here
would be the one nobody notices is stale. Asking the source costs one request per
changed id and cannot drift.

**Rejected: polling the graphs endpoint per interval.** That was the fallback before
graph events existed. It stays as the safety net, but as the only mechanism it makes
the latency of noticing a user's edit equal to the interval.

## One instance, no lease

See [architecture.md](architecture.md). The alternative is a lease, which is shared
state, which is the database this design does without.

## Structure is generated once, then only extended

The full heuristic runs when a group's graph is created. Afterwards the service adds
and removes device nodes and never re-parents an existing edge, even when newer
readings suggest a better tree.

Silently undoing a user's manual correction is worse than leaving a suboptimal guess in
place. The heuristic is a guess (see [known-limitations.md](known-limitations.md)); a
user's correction is knowledge.

**Rejected: recomputing the structure when the readings change enough.** Without a
database there is also nowhere for a better proposal to wait between two runs — it
would have to be recomputed on demand — and it would still have to decide whether to
overwrite a user, which is the question above.

## Only two synthetic nodes, and the second one earns it

Every intermediate node in a generated graph is a meter that actually measures. The
service invents no grouping levels from locations or from name patterns: a level a user
cannot verify against a reading is a level they have to undo.

The root is one exception by necessity — the graph model demands a single sink that is
not a device. The second is the collector for devices whose type measures no medium at
all. It was added after a real site came out as one meter and eighty-five contacts,
motion sensors and remotes side by side at the root, which is technically correct and
useless to look at. Those devices still appear — every device must be in its company's
graph — but they no longer crowd out the thing the graph is for. Its label is
configurable and it carries a translation key so the graph view can localise it.

A device that could have reported a reading and did not is deliberately **not**
collected there: it belongs in the flow, and it hangs off the root where it is visible.

**Rejected: grouping levels from platform locations.** The service does not read
locations and derives nothing from them. A location is where a device is, not what
feeds it, and the two coincide often enough to look convincing and rarely enough to
mislead.

## The technical user owns the graph, the group may only edit

The configured `service_user_id` is `Graph.Owner` and the graph's only administrator.
The group gets read, write and execute — members may edit, but may not delete the
company graph or re-share it to someone else.

This also satisfies the permissions model: a resource's permissions must name at least
one user with `administrate`, so a graph shared only with a group would be invalid.

## The group id is the identity, the path is not

Keycloak rewrites a group's path when the group is renamed. A graph found by path would
be orphaned on a rename and a second one created beside it — and the first one carries
everything the user had built. The id changes for nothing short of deleting the group.
The path is kept beside it because sharing needs it.

## Group changes are polled

The realm has no admin event listener, so there is no event stream for group CRUD. The
poll interval is therefore also the worst-case latency for noticing a new company.

## Deliberately out of scope

- **Graphs created by users.** The service neither reads nor modifies them. That is a
  decision about what this service is for, not a limit on what it could reach: its
  token would be allowed to write them.
- **Deleting graphs of deleted groups.** The group attribute and the share are removed;
  the graph stays. Deleting a user's work in response to a directory event is the
  harsher of the two actions, and the graph remains usable to its owner.
- **Edge weights below 100.** A tree does not need them. As soon as a device may have
  two parents, that becomes its own subject.
