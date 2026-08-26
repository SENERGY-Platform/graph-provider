# Known limitations

What the service gets wrong, or cannot know, on purpose. Each of these has been
considered and left as it is — reading one as a defect and fixing it is likely to break
something the reason names.

## Scope

Holds for the service as built. Each item names the condition under which it applies;
several are properties of a foreign system rather than of this code, and those say so.

**Not this if**: the behaviour you are looking at is not in this list. Then it is a bug
and not a documented trade-off.

**Breadth**: mixed, marked per item. The heuristic's four failure modes are
`einzelfall` — they were found against real sites.

## The heuristic can be wrong

Best-fit containment on a single window is a guess. It is applied once, marked as a
system change, and never re-applied over a user's correction. Four ways it is known to
be wrong, none of them fatal because a user can correct the result:

- **Tightest fit is not deepest fit, and that is the decision.** With a 1000 main
  meter, a 600 sub-meter under it and a 200 to place, the 1000 has 400 unexplained and
  the 600 has 600, so the tighter container is the 1000 and the 200 becomes its sibling
  rather than its child. Both are physically possible. Least-unexplained was chosen over
  most-depth deliberately: a guessed level is work a user has to undo, and the flatter
  tree guesses less. `TestBuildTightestFitWinsOverDepth` pins the consequence, so it is
  not later mistaken for a defect and "fixed".
- **A device that reads two carriers is placed by the larger figure**, and those figures
  are compared across carriers whose units differ. A plant whose gas is metered in one
  unit and electricity in another can end up in the wrong carrier's tree.
- **A partially reporting three-phase device reads low.** Only phases that reported at
  both ends of the window are summed, and the shortfall is indistinguishable downstream
  from a genuinely smaller consumer, so such a device may be placed under a parent that
  is too tight for it.
- **Zero and negative readings say nothing about containment.** A meter that did not
  move over the window, and one whose counter was exchanged or rolled over, are both
  treated as having no reading at all: attached to the root, never offered as a parent.
  Zero is not a small number here — an empty container would otherwise accept every
  device, and a whole site of idle meters came out as an invented hierarchy under
  whichever device sorted first.

## One malformed device type costs a whole batch of readings

Column and service names come from device types and are validated by the timescale
wrapper, which rejects an **entire request** when one element is invalid. There is no
injection risk — the wrapper rejects rather than interpolates — but a single bad type
denies the readings of every device batched with it, and those devices then land at the
root. The carrier analysis filters the known-bad shape out beforehand; see
[structure-heuristic.md](structure-heuristic.md).

## A group rename costs the company its device access, platform-wide

Keycloak rewrites the group path; the permissions service keys every device's group
rights by the old path and is not told. That is not this service's doing and it cannot
fix it. What it does is refuse to draw a conclusion from it — see the rename safeguard
in [reconciliation.md](reconciliation.md).

## Group changes are polled

The realm has no admin event listener, so there is no event stream for group CRUD. The
poll interval is the worst-case latency for noticing a new company.

## An unreachable broker looks healthy at first

The consumer library connects lazily, so a wrong broker address cannot be reported at
startup. See [operating.md](operating.md) for what is reported instead and why a bad
record is skipped rather than retried.

## Graph events come from the store, not from here

The graphs topic and its content commands are configured and published in the
device-repository. Should they not be published — an older version — the service still
works and finds the same facts on the next full pass. Only the latency changes. The
reason it is not configured from here is in [reconciliation.md](reconciliation.md).

## A masked string type does not survive fmt in an unexported field

`fmt` will not call `String()` on a value it reached through an unexported field and
prints the raw characters instead, so a secret held that way leaks under `%v`.
Credentials are therefore held in closures, which reflection cannot reach. The rule
still binds the caller: reading a credential out and logging it defeats all of this.
