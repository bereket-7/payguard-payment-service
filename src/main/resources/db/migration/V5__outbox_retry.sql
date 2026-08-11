-- Per-entry retry state for the outbox relay.
--
-- The relay previously wrapped the whole batch in one transaction and rethrew on the first failure,
-- so a single unpublishable payload rolled back every sibling entry and was re-read on the next
-- 1s poll — the events queued behind it never went out. Retry state now lives on the row: a failing
-- entry is pushed forward by an exponential backoff, and once it exhausts its attempts it is copied
-- to the dead-letter topic and marked so the relay stops considering it.

alter table outbox add column attempts integer not null default 0;
alter table outbox add column last_error text;
alter table outbox add column next_attempt_at timestamptz not null default now();
alter table outbox add column dead_lettered_at timestamptz;

-- Backfill: rows written before this migration are due immediately.
update outbox set next_attempt_at = created_at where published_at is null;

-- The relay orders by (next_attempt_at, created_at) and filters out published and dead-lettered
-- rows, so the old created_at-only partial index no longer covers the claim query.
drop index if exists idx_outbox_unpublished;
create index idx_outbox_publishable
    on outbox (next_attempt_at, created_at)
    where published_at is null and dead_lettered_at is null;

-- Operators need to find poisoned events without scanning the whole table.
create index idx_outbox_dead_lettered
    on outbox (dead_lettered_at desc)
    where dead_lettered_at is not null;
