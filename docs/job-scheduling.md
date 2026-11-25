# Scheduled queue jobs

Trusted operator-side integrations can submit a job with an absolute deadline
and an optional start time through the worker control API. Both timestamps must
use RFC 3339 UTC (`Z` or `+00:00`). A deadline must be in the future and no more
than 72 hours after the job is created; a scheduled start must be before that
deadline. The worker stores both values in the same queue transaction, so a
scheduled job is not claimable before its start time.

The `set-deadline` action accepts an absolute deadline and cannot shorten the
current deadline or exceed the 72-hour per-job limit. The `reschedule` action
can move only queued or waiting jobs. It changes only the job's next start time;
provider cooldown and quota state are left intact. Repeating either action with
the same absolute value is idempotent. A reschedule also makes that time
operator-owned when it equals an existing provider wait; resetting a profile
preserves operator-owned times but releases provider-owned waits.

Queue schema v3 persists schedule ownership. Queue schema v4 stores cancellation
tombstones for broker idempotency keys, preventing an unbound submission from
being accepted after the broker has confirmed cancellation. During upgrade,
existing schedule ownership is reconstructed from the latest relevant event by sequence number. Earlier
versions did not record a reschedule equal to a provider wait, so that historical
operator intent cannot be recovered: such a wait is treated as provider-owned.
Operators should review and reapply those schedules before a profile reset.
Older workers reject newer queue schemas; take a pre-upgrade backup before upgrading.
For a coordinated worker/broker upgrade and rollback, including the v4 queue,
backup validation, job reconciliation, and mixed-runtime prohibition, see
[`docs/ops/queue-v4-upgrade-rollback.md`](ops/queue-v4-upgrade-rollback.md).

Scheduled submission and schedule changes require the worker's operator
identity. Ordinary callers cannot set a deadline or start time and cannot use
the administrative queue actions. The broker remains responsible for source
allowlists, request ownership, and its own audit trail; do not expose the worker
socket or queue database to untrusted callers.
