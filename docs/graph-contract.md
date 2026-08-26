# Graph contract: model, frontend conventions, provenance

What a generated graph must look like to be accepted by the store, to render correctly
in the graph view, and to be recognised as its own work by a later pass.

## Scope

Holds for every graph this service writes. Three separate contracts, with three
different enforcers: the model's validation (a rejected write), the graph view's
conventions (a wrong picture, no error), and this service's own attributes (a duplicate
graph beside the user's work).

**Not this if**: the graph was created by a user. The service neither reads nor writes
those, and none of this is asserted about them.

**Breadth**: `allgemein` for the model constraints, which are enforced by the model's
own validation. `mehrfach` for the frontend conventions — they are read from the graph
view's code, not from a schema, and nothing but this document keeps the two in step.

## Model constraints

The store rejects a graph that violates any of these, so the generator must not produce
one:

- exactly **one** node without an outgoing edge, and it may not be a device node — the
  root is always a custom node;
- edge weights 1..100; outgoing weights per node sum to 0 or 100;
- no cycles, no duplicate resource id within a graph;
- an owner is required.

In a tree every node has exactly one parent, so every weight is 100 and the heuristic
never has to estimate a share.

## Frontend conventions

The graph view reads a graph by conventions that are **not expressed in any schema**. A
generated graph must follow them or it renders wrongly, with no error anywhere:

- **Edges point child to parent.** The root is the source, devices are the sinks.
- **A graph's display name is the `name` attribute of its root node**, not a
  graph-level attribute.
- **Device nodes use `id == resource_id`.** A grouping node gets a fresh UUID; the root
  is called `root`.
- **`system_changed`**, on the graph or on an edge, asks the user to re-check the
  shares. Everything this service touches is marked with it.

This is a contract without a schema, in both directions: a change to these conventions
on either side breaks the other silently, and there is no test on either side that
catches it. A change here is therefore a change to something outside this repository,
and the graph view has to be checked against it.

## Provenance attributes

Written by the service onto every graph it creates, and the only record it keeps of its
own work:

```text
{key: "graph-provider/keycloak-group-id", value: "<keycloak group id>", origin: "graph-provider"}
{key: "graph-provider/keycloak-group",    value: "/path/of/group",      origin: "graph-provider"}
{key: "graph-provider/name",              value: "<name last written>", origin: "graph-provider"}
```

- **The group id is the identity.** A rename rewrites the path; a graph found by path
  would be orphaned and a second one created beside the one holding the user's work.
- **The path is kept beside it because sharing needs it**: group permissions are keyed
  by the full path, matching the token's `groups` claim. On a rename the id still
  matches, the path attribute is rewritten, the new path is shared and the old one
  withdrawn — in that order, so a failure between the two cannot leave the company with
  no access to its own graph.
- **The name answers the one question that needs history**: does the root still carry
  the name the service wrote, or did a user rename the unit?

### Finding them costs one listing per key

Not one listing naming both. The store's `attributes` filter is documented as matching
an element that carries *any* of the listed keys; it does not. One `$elemMatch` per
attribute is joined by `$and`, so naming both keys returns only graphs carrying both —
and a graph carrying only the path, one written before the id was recorded or a write
interrupted between the two, would be invisible. The service would then build a second
graph beside the one holding the user's work. Two queries, merged, is the price of not
doing that.

`attributes_json`, the filter that would match attribute *values*, is not an
alternative: the store's Go client percent-encodes the JSON and the query encoder
escapes it again, so what arrives is no longer JSON:

```
unexpected statuscode 400: unable to parse attributes_json:invalid character '%' looking for beginning of value
```

Both are asserted on their own in the integration suite and named in the failure, so a
later fix on either side shows up as a refuted assumption rather than as a mystery. See
[testing.md](testing.md).
