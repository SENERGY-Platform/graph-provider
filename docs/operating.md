# Operating the service

Running it, looking before writing, and what its own health reporting does and does not
say.

## Scope

Holds for a single running instance, which is the only supported topology
([architecture.md](architecture.md)). The two ways of looking without writing — the kill
switch and the dry run — answer different questions and are not interchangeable.

**Not this if**: the broker address is wrong. That is the one misconfiguration this
service cannot report at startup, see below; nothing in `/info` will show it and only
the consumer library's own log lines will.

**Breadth**: `mehrfach`.

## Run

```
go build -o graph-provider . && ./graph-provider -config config.json
```

## Looking before writing

**`ENABLED=false` is the kill switch**: everything runs, reads and computes, and nothing
is written. Use it for a first look at a new environment — does the service see the
groups, the devices per group and the carrier columns correctly?

**`-dry-run` answers the question the kill switch does not**: *what would this service
write to my graphs?* It runs one reconciliation pass against the real cluster, prints
the graph it would create or change for every group, and writes nothing — regardless of
`ENABLED`. No Kafka consumer is started, so no offset is committed, and no HTTP server
is started, so an orchestrator cannot mistake it for the running instance.

It needs everything the service needs except Kafka. Those stay required: a dry run that
silently read no groups would print an empty report, which is exactly what a platform
with nothing to do prints.

The report goes to stdout and the log records go to stderr, so the report can be piped.
Exit code 0 means the pass completed, 1 means it failed — the report is printed either
way, because a pass that failed over one group still worked out the others.

```
./graph-provider -config config.json -dry-run > plan.txt
```

```
what a reconciliation pass would write
=====================================

1. create  group /acme/plant-1  graph "plant-1" (new)
  4 devices, 1 of them without a reading
  - plant-1
    - Hauptzaehler (d1)
      - Halle Ost (d2)
        - Presse 3 (d3)
    - Kompressor (d4)  [no reading in the window, so it hangs off the root]

2. share  group /beta  graph "beta" (g12)
  group /beta

summary
  groups affected: 2
  graphs to create: 1
  graphs to update: 0
  device nodes in those graphs: 4
  devices at the root for want of a reading: 1
```

## HTTP

| Route | Purpose |
| --- | --- |
| `GET /health` | liveness |
| `GET /info` | version, whether writing is enabled, whether the trigger consumers are alive, queue and cache sizes |
| `POST /reconcile` | ask for a full pass; answers 202, does not wait |
| `POST /reconcile/group?path=/acme` | ask for one group |

The routes are annotated with swaggo comments in `pkg/api/api.go`. `go generate ./...`
writes `graphprovider_swagger.json`, `graphprovider_swagger.yaml` and
`graphprovider_docs.go` into this folder. Nothing in the service serves them over HTTP;
they are committed so the API can be read without building anything.

## An unreachable broker looks healthy at first

The consumer library connects lazily and reconnects indefinitely, so nothing about a
wrong Kafka address can be reported when the consumers are started — the error `Start`
returns cannot carry it.

What *is* reported is a consumer that has **ended**: that is a distinct signal, it is
logged at error level, it shows as `kafka_alive: false` on `/info`, and it makes the
process exit so the orchestrator restarts it. A broker that was never reached leaves
nothing ended, so only the library's own log lines show it.

The other half of that contract: a message that cannot be used is skipped and logged as
a warning, and consumption continues. Returning an error there would make the library
retry for ten minutes and then kill the consumer, so one bad record would cost the
trigger stream. A rebalance now costs a process restart, which is loud but honest — the
alternative was a permanently degraded process reporting itself well.

## Restart behaviour

No database. Everything held is derived from the device-repository, permissions-v2 and
Keycloak, and a restart rebuilds it. What the service needs to know about its own
earlier work lives on the graph, as attributes. The Kafka consumer group is stable, so a
restart resumes where it stopped.
