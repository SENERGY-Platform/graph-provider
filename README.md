# graph-provider

Maintains one default energy-flow graph per Keycloak group that owns at least one
device: named after the group, containing every device the group has access to,
structured by what the meters actually read, and shared back with the group.

Every graph is built by hand otherwise. A site with ninety meters is ninety manual
assignments, and a newly provisioned device belongs to no graph at all, so it silently
drops out of every evaluation.

## Run

```
go build -o graph-provider . && ./graph-provider -config config.json
```

`config.json` holds the defaults; every value can be overridden by the environment
variable named in its `env_var` tag. Eight values have no default and the service
refuses to start without them.

Two ways to look before writing anything: `ENABLED=false` reads and computes and writes
nothing, and `-dry-run` prints the graph it would create or change for every group.
Both are described in [docs/operating.md](docs/operating.md).

For a local run, copy `.env.example` to `.env` and fill it in — the VSCode launch
configurations read it, and it is gitignored because it carries the Keycloak client
secret. That file also documents the two traps that cost the most time: the
cluster-internal addresses, and the Keycloak service-account roles.

## Test

```
go test ./...
go test -race ./...
```

Every package is tested without a network. The suite that talks to a real
device-repository, a real permissions-v2 and a real Kafka needs Docker and is behind a
build tag:

```
go test -tags integration -count=1 -timeout 900s ./pkg/tests/
```

## Documentation

[docs/](docs/) holds the technical documentation. Read the one you need; nothing here
depends on reading them in order.

| Document | |
| --- | --- |
| [architecture.md](docs/architecture.md) | packages, trigger sources, what is held in memory, why one instance |
| [design-decisions.md](docs/design-decisions.md) | the load-bearing decisions and the alternatives that were dropped |
| [structure-heuristic.md](docs/structure-heuristic.md) | carriers, columns, the consumption query recipe, sub-metering detection |
| [graph-contract.md](docs/graph-contract.md) | model constraints, the graph view's unwritten conventions, provenance attributes |
| [reconciliation.md](docs/reconciliation.md) | what a pass does to an existing graph, and what it refuses to conclude |
| [tokens-and-permissions.md](docs/tokens-and-permissions.md) | which token opens which system, the Keycloak roles, what is granted on a graph |
| [configuration.md](docs/configuration.md) | every key and its reason, and the values with no default |
| [operating.md](docs/operating.md) | kill switch, dry run, HTTP routes, what the health reporting does not say |
| [testing.md](docs/testing.md) | what is covered where, and what the integration suite is for |
| [known-limitations.md](docs/known-limitations.md) | what the service gets wrong on purpose |

`docs/graphprovider_swagger.json`, `.yaml` and `graphprovider_docs.go` are generated
from the swaggo comments in [pkg/api/api.go](pkg/api/api.go) by `go generate ./...` and
committed alongside. Nothing in the service serves them over HTTP.

## Layout

`main.go` in the root, `pkg/` beside it — one package per responsibility, listed in
[docs/architecture.md](docs/architecture.md).
