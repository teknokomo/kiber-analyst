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
the same absolute value is idempotent.

Scheduled submission and schedule changes require the worker's operator
identity. Ordinary callers cannot set a deadline or start time and cannot use
the administrative queue actions. The broker remains responsible for source
allowlists, request ownership, and its own audit trail; do not expose the worker
socket or queue database to untrusted callers.
