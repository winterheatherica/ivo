-- The imaging schema, matching docs/detail/postgres-schema.md.

CREATE TABLE imaging.outbox (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id      uuid        NOT NULL UNIQUE,
    destination   text        NOT NULL DEFAULT 'kafka'
                              CHECK (destination IN ('kafka', 'rabbitmq')),
    topic         text        NOT NULL,
    message_key   text        NOT NULL,
    event_type    text        NOT NULL,
    payload       bytea       NOT NULL,
    headers       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    player_id     uuid,
    occurred_at   timestamptz NOT NULL,
    available_at  timestamptz NOT NULL,
    published_at  timestamptz,
    attempts      integer     NOT NULL DEFAULT 0,
    last_error    text
);
CREATE INDEX outbox_due_idx ON imaging.outbox (available_at, id) WHERE published_at IS NULL;
CREATE INDEX outbox_cleanup_idx ON imaging.outbox (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX outbox_player_pending_idx ON imaging.outbox (player_id) WHERE published_at IS NULL;

CREATE TABLE imaging.processed_events (
    consumer      text        NOT NULL,
    event_id      uuid        NOT NULL,
    processed_at  timestamptz NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_cleanup_idx ON imaging.processed_events (processed_at);

CREATE TABLE imaging.render_catalog (
    item_id        text   PRIMARY KEY,
    kind           text   NOT NULL CHECK (kind IN ('outfit', 'pose', 'expression')),
    positive_tags  text[] NOT NULL,
    negative_tags  text[] NOT NULL DEFAULT '{}'
);

CREATE TABLE imaging.image_batches (
    batch_id       uuid          PRIMARY KEY,
    lane           text          NOT NULL CHECK (lane IN ('batch', 'express')),
    modal_call_id  text,
    status         text          NOT NULL CHECK (status IN ('running', 'done', 'failed', 'timeout')),
    job_count      integer       NOT NULL CHECK (job_count > 0),
    cold_start     boolean,
    gpu_seconds    numeric(12,3),
    started_at     timestamptz   NOT NULL,
    finished_at    timestamptz
);
CREATE INDEX image_batches_running_idx ON imaging.image_batches (started_at)
    WHERE status = 'running';

CREATE TABLE imaging.image_jobs (
    job_id          uuid          PRIMARY KEY,
    post_id         uuid          NOT NULL,
    player_id       uuid          NOT NULL,
    hold_id         uuid          NOT NULL UNIQUE,
    variant         integer       NOT NULL DEFAULT 0 CHECK (variant >= 0),
    lane            text          NOT NULL CHECK (lane IN ('batch', 'express')),
    status          text          NOT NULL CHECK (status IN
                                  ('queued', 'dispatched', 'completed', 'review', 'rejected', 'failed')),
    render_spec     jsonb         NOT NULL,
    retry_count     integer       NOT NULL DEFAULT 0 CHECK (retry_count BETWEEN 0 AND 3),
    batch_id        uuid          REFERENCES imaging.image_batches (batch_id),
    attempts        integer       CHECK (attempts BETWEEN 1 AND 3),
    gpu_seconds     numeric(12,3),
    nsfw_scores     jsonb,
    image_tags      text[],
    output_s3_key   text,
    output_sha256   bytea,
    failure_reason  text          CHECK (failure_reason IN
                                  ('nsfw_rejected', 'review_rejected', 'review_expired',
                                   'technical', 'hold_failed')),
    created_at      timestamptz   NOT NULL,
    dispatched_at   timestamptz,
    finished_at     timestamptz,
    UNIQUE (post_id, variant),
    CHECK (status <> 'dispatched' OR batch_id IS NOT NULL),
    CHECK (status <> 'completed' OR output_s3_key IS NOT NULL)
);
CREATE INDEX image_jobs_dispatched_idx ON imaging.image_jobs (dispatched_at)
    WHERE status = 'dispatched';
CREATE INDEX image_jobs_batch_idx ON imaging.image_jobs (batch_id);
CREATE INDEX image_jobs_player_idx ON imaging.image_jobs (player_id);
CREATE INDEX image_jobs_queued_idx ON imaging.image_jobs (lane, created_at)
    WHERE status = 'queued';

CREATE TABLE imaging.review_items (
    job_id      uuid        PRIMARY KEY REFERENCES imaging.image_jobs (job_id) ON DELETE CASCADE,
    s3_key      text        NOT NULL,
    scores      jsonb       NOT NULL,
    created_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,
    decision    text        CHECK (decision IN ('approved', 'rejected', 'expired')),
    decided_by  uuid,
    decided_at  timestamptz,
    CHECK ((decision IS NULL) = (decided_at IS NULL))
);
CREATE INDEX review_items_open_idx ON imaging.review_items (expires_at)
    WHERE decision IS NULL;
