# Architecture

One default energy-flow graph per Keycloak group: named after the group, holding every
device the group has access to, structured by what the meters actually read, and shared
back with the group.

## Scope

Holds for this service as it is built: several trigger sources feeding one
reconciliation, no database, a single writing instance. The package boundaries below
are the ones in `pkg/`.

**Not this if**: you are looking for *why* a decision came out this way — that is
[design-decisions.md](design-decisions.md). For how a tree is derived from readings,
see [structure-heuristic.md](structure-heuristic.md); for what a pass does to an
existing graph, [reconciliation.md](reconciliation.md).

**Breadth**: `allgemein` — this is the design of the service, not an observation about
it.

## Packages

```text
main.go
pkg/
  model/         the types every other package speaks in
  config/        configuration, loaded in one place
  carrier/       which carrier a device type reads, and in which column
  consumption/   consumption per device and carrier, from the timescale wrapper
  structure/     readings to a tree: sub-metering detection
  graphs/        reading, writing and sharing graphs - the only writer
  cache/         in-memory views of devices, groups and readings
  keycloak/      the group tree
  events/        Kafka, as a trigger and nothing else
  reconcile/     the pass that puts it together
  api/           health, info, manual reconcile
  plan/          renders what a pass would do, for the dry run
  tests/         the suite that talks to the real platform
```

## External interfaces

| System | Used for | Access |
| --- | --- | --- |
| device-repository | `/graphs` CRUD, `/extended-devices` with full device types | Go client, cluster-internal |
| permissions-v2 | device to group permissions, graph permissions | Go client, cluster-internal |
| timescale-wrapper | `/queries/v2` consumption readings | Go client, cluster-internal |
| Keycloak Admin API | group tree | client credentials |
| Kafka | `devices`, `device-types`, `graphs` | trigger only |

## Many triggers, one reconciliation

Kafka messages and the Keycloak poll only mark groups as dirty. Reconciliation runs in
one place, serially per group, and is idempotent — which is what lets a backlog after a
restart and a live event take the same code path.

| Trigger | Source | Effect |
| --- | --- | --- |
| Device created, changed, deleted | Kafka `devices`, `PUT`/`DELETE` | invalidate device, mark its groups dirty |
| Device shared with or removed from a group | Kafka `devices`, `RIGHTS` | invalidate device, mark old and new groups dirty |
| Device type changed | Kafka `device-types` | invalidate carrier columns of affected devices |
| Graph changed or deleted | Kafka `graphs` | mark the graph's group dirty |
| Group created, renamed, deleted | Keycloak Admin API, interval | rebuild group tree |
| Safety net | interval and startup | reconcile every group |

**Only the id is read from a Kafka message**, never the body. The service then asks the
authoritative source: `ListExtendedDevices({Ids, FullDt: true})`,
`GetResource("devices", id)`, or `ListGraphs({Ids})` for a graph.

**The handler hands the trigger on and keeps reading.** It records the id in a set of
pending work and returns immediately; reconciliation runs in its own loop. A handler
that waited would stall the partition, and a pass touches four foreign systems and can
take seconds. Because the set deduplicates, a backlog from an outage costs one pass per
affected group rather than one per message.

**The consumer group is stable and comes from configuration**, so committed offsets
decide where reading resumes. A restart continues where it stopped. No start offset is
set: on its very first run the group reads the whole compacted history, which costs one
pass of triggers over every resource and settles. Nothing relies on where reading
begins, because the startup sweep covers everything that came before it.

## What is held in memory

Four views, all derived, all rebuilt at startup:

- **Group tree** — path, Keycloak id, derived name. From the Keycloak Admin API,
  replaced wholesale on every poll.
- **Devices** — id, name, device type id, derived carriers and columns. From
  `ListExtendedDevices(FullDt: true)`, paged.
- **Device to groups** — from `ListResourcesWithAdminPermission("devices")`, paged;
  per device the group permissions holding at least `read`.
- **Readings** — per device and carrier the last derived consumption with its window
  and query time, with a TTL. Keeps a tick from hitting the timescale wrapper without
  need.

**The graph itself is not mirrored.** It is fetched fresh per pass through
`ListGraphs`, so a user's edit is never computed against a stale copy. What the service
needs to know about its own earlier work lives on the graph as attributes, see
[graph-contract.md](graph-contract.md).

## One instance

Reconciliation is the only writer to a graph, and without a database there is nowhere
to hold a lease; two replicas would write the same graph concurrently and overwrite
each other. The Kafka side would scale — the consumer group divides partitions — but
the writer does not, so scaling out would require a lease and therefore shared state
again.
