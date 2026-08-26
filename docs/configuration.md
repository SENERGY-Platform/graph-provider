# Configuration

One configuration struct, a JSON file plus an environment overlay. Nothing else in the
service reads the environment.

## Scope

Holds for `pkg/config`. Every value can be overridden by the environment variable named
in its `env_var` tag; `config.json` holds the defaults. The list below is the reason for
each key, not a duplicate of the struct — the struct is the reference.

**Not this if**: a value appears to be ignored. An empty environment variable is not
the same as an unset one: an empty value is applied, and for a numeric or boolean field
that is a startup failure rather than a fallback. Leave a variable out rather than
setting it blank.

**Breadth**: `allgemein`.

## Keys

| Key | Purpose |
| --- | --- |
| `device_repository_url`, `permissions_v2_url`, `timescale_wrapper_url` | cluster-internal, not the gateway |
| `kafka_url`, `kafka_consumer_group`, `device_topic`, `device_type_topic`, `graph_topic` | triggers; the consumer group is stable and must not differ per instance, and the three topics must name three different topics |
| `keycloak_url`, `keycloak_realm`, `keycloak_client_id`, `keycloak_client_secret` | group tree |
| `service_user_id` | graph owner; should be the `sub` of the token in use |
| `group_poll_interval`, `reconcile_interval` | default 5 min / 1 h |
| `reading_ttl` | default 24 h, lifetime of a cached consumption value |
| `reference_window`, `containment_tolerance` | heuristic, default 720 h / 0.05 |
| `no_flow_node_name` | label of the collector for devices measuring no medium; empty switches it off |
| `group_include`, `group_exclude` | path filters, to keep platform-internal groups out |
| `enabled` | kill switch: read and compute, write nothing |
| `server_port`, `http_timeout`, `http_access_log` | HTTP server |
| `logger` | handler, level, time format |

## Values with no default

The service refuses to start without these, because every one of them has a wrong
answer that fails silently rather than loudly:

`DEVICE_REPOSITORY_URL`, `PERMISSIONS_V2_URL`, `TIMESCALE_WRAPPER_URL`, `KAFKA_URL`,
`KEYCLOAK_URL`, `KEYCLOAK_CLIENT_ID`, `KEYCLOAK_CLIENT_SECRET`, `SERVICE_USER_ID`.

Two of them are easy to get wrong:

- **The service addresses must be cluster-internal, not the API gateway.** See
  [tokens-and-permissions.md](tokens-and-permissions.md).
- **`SERVICE_USER_ID` is the owner written onto every graph the service creates.** The
  service warns at startup when it is not the subject of the token, because that is the
  likely typo — it does not refuse, since a dedicated technical user is a legitimate
  setup.

## The Keycloak client secret

Held as a masked type so it cannot reach a log or a JSON dump. Copy `.env.example` to
`.env` for a local run; `.env` is gitignored because it carries that secret.
