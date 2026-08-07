create table processed_webhooks (
  id uuid primary key,
  stripe_event_id varchar(255) not null unique,
  event_type varchar(128) not null,
  processed_at timestamptz not null
);
