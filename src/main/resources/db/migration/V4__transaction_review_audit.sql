create table transaction_review_audit (
  id uuid primary key,
  transaction_id varchar(64) not null,
  merchant_id varchar(128) not null,
  action varchar(16) not null,
  actor_sub varchar(255) not null,
  created_at timestamptz not null
);

create index idx_review_audit_transaction on transaction_review_audit (transaction_id, created_at);
