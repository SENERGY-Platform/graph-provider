# Tokens and permissions

Which token opens which system, what the service needs granted, and what it grants on
the graphs it creates.

## Scope

Holds for the four systems the service authenticates against: the device-repository,
permissions-v2 and the timescale wrapper with one internal admin token, and the
Keycloak Admin API with client credentials. The Keycloak role names are those of the
client that administers the realm.

**Not this if**: the call goes through the API gateway. The internal admin token is
rejected there, and the symptom is an authentication error that looks like a wrong
token and is not — see the first trap below.

**Breadth**: `mehrfach` for the token model, which follows from how the three services
parse tokens. `einzelfall` for the Keycloak role observations: they were met in one
realm, on one version, and the role layout of another realm may differ.

## One token for three services

The device-repository, permissions-v2 and the timescale wrapper all parse bearer tokens
without verifying the signature, and the realm role `admin` holds `rwxa` on every
device through the device topic's initial group rights. The internal admin token from
the permissions-v2 client is therefore sufficient for all three.

**It is rejected at the API gateway, so every call must go cluster-internal.** This is
the first thing to check when a fresh deployment cannot read anything: the addresses
must be the in-cluster service addresses, not the public ones.

**permissions-v2 returns early for a token carrying the `admin` role.** Every
permission check — reading a resource, writing one, deleting one — short-circuits to
allowed before the resource's own rights are consulted. So the service can manage any
graph, not only the ones it owns. It confines itself to its own by decision, not by
lack of permission.

## service_user_id

The owner written onto every graph the service creates, and therefore its only
user-level administrator. It should be a subject the platform recognises. The service
**warns** at startup when it is not the token's own subject, because that is the likely
typo, and does not refuse: a dedicated technical user that is not the token's subject
is a legitimate setup, and the admin role means the service can still manage what it
created.

For the internal admin token the subject is `dd69ea0d-f553-4336-80f3-7f4567f85c7b`.

## The Keycloak Admin API needs real credentials

Client credentials, and a service account holding **`view-users` and `query-groups`**
from the client that administers the realm — `<realm>-realm`, so `master-realm` for the
master realm. Three traps, all met in practice:

- **`realm-management` does not exist in the master realm.** A role taken from another
  realm's `<realm>-realm` client grants nothing here and produces a 403 with a
  perfectly valid token.
- **`view-groups` is the wrong role.** It belongs to the `account` client and is about
  a user seeing their own group memberships. In the admin model groups sit under the
  *users* scope, so `view-users` is what opens the group tree.
- **With `query-groups` alone the listing comes back as an empty array with HTTP 200**,
  because the listing is filtered by what the caller may view. That is
  indistinguishable from a realm with no groups, and taken at face value it would look
  like every company having disappeared. The client compares the listing against
  `/groups/count` and **refuses** rather than believing it:

  ```
  keycloak reports 12 group(s) in realm master but showed none: the service account
  may query groups but not view them. Grant view-users on the client that administers
  this realm ("master-realm") - view-groups is a role of the account client and is not
  what governs the admin API
  ```

  This has to be an error and not an empty result, because everything downstream
  replaces its view of the world with what the listing returns.

The same reasoning applies to pagination: a page shorter than `max` is not proof of the
last page, because the listing is filtered after the limit is applied. One extra
request per listing, and a bound on the loop.

## What the service grants on its own graphs

On creation, and on any pass where the actual state differs:

```text
UserPermissions  { <service_user_id>: r w x a }
GroupPermissions { <group path>:      r w x - }
```

The group can edit the graph but not delete or re-share it. Group keys are full paths
with a leading `/`, matching the `groups` claim.

When a group disappears from Keycloak, its entry is removed from the graph's group
permissions and its group attribute is cleared. The graph is not deleted.

## Secrets

The Keycloak client secret is held so that it cannot reach a log. The masked config
type covers the configuration struct; inside the client the credential is held in a
closure, because `fmt` will not call `String()` on a value it reached through an
unexported field and prints the raw characters instead. The rule still binds the
caller: reading a credential out and logging it defeats all of it.
