# Postgres schema

Aurora PostgreSQL is shared, but every service has its own schema and role, with no foreign keys or queries across schemas ([design §14](../system-design.md#14-data-storage)). The `auth` schema belongs to the auth server from my thesis and isn't detailed here.

Rules for the SQL blocks on this page:

- A plain block is DDL that runs in order.
- A block that starts with `-- template:` is repeated for every schema it names, with `{{schema}}` replaced by the schema name.
- A block that starts with `-- aurora-only` only applies on Aurora (IAM roles).
- A block that starts with `-- example` is an example query with `$n` parameters, not DDL.

No time column has `DEFAULT now()`, because time always comes from `Clock` in the application.

## Schemas and roles

```sql
CREATE SCHEMA game;
CREATE SCHEMA billing;
CREATE SCHEMA imaging;
CREATE SCHEMA channels;
CREATE SCHEMA ops;
CREATE SCHEMA analytics;
```

```sql
-- aurora-only: one role per service, logging in with IAM authentication
CREATE ROLE svc_game LOGIN;
GRANT rds_iam TO svc_game;
ALTER SCHEMA game OWNER TO svc_game;
ALTER ROLE svc_game SET search_path = game;
-- Repeated for svc_billing, svc_imaging, svc_channels, svc_ops, and svc_analytics.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
```

For a portfolio, one role per service is enough. Migration and runtime roles can be split later without changing any table.

## Outbox and `processed_events`

Every schema has these two tables. Their code is in `pkg/outbox` and is shared by every Go service that uses Postgres.

```sql
-- template: game, billing, imaging, channels, ops, analytics
CREATE TABLE {{schema}}.outbox (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id      uuid        NOT NULL UNIQUE,
    destination   text        NOT NULL DEFAULT 'kafka'
                              CHECK (destination IN ('kafka', 'rabbitmq')),
    topic         text        NOT NULL,   -- Kafka topic, or RabbitMQ routing key
    message_key   text        NOT NULL,
    event_type    text        NOT NULL,   -- ce_type
    payload       bytea       NOT NULL,   -- binary Protobuf
    headers       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    player_id     uuid,                   -- so delayed rows can be deleted with the account
    occurred_at   timestamptz NOT NULL,
    available_at  timestamptz NOT NULL,
    published_at  timestamptz,
    attempts      integer     NOT NULL DEFAULT 0,
    last_error    text
);
CREATE INDEX outbox_due_idx ON {{schema}}.outbox (available_at, id)
    WHERE published_at IS NULL;
CREATE INDEX outbox_cleanup_idx ON {{schema}}.outbox (published_at)
    WHERE published_at IS NOT NULL;
CREATE INDEX outbox_player_pending_idx ON {{schema}}.outbox (player_id)
    WHERE published_at IS NULL;

CREATE TABLE {{schema}}.processed_events (
    consumer      text        NOT NULL,   -- <service>.<topic>
    event_id      uuid        NOT NULL,
    processed_at  timestamptz NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_cleanup_idx ON {{schema}}.processed_events (processed_at);
```

```sql
-- example: the relay picks rows that are due
SELECT id, event_id, destination, topic, message_key, event_type, payload, headers
  FROM game.outbox
 WHERE published_at IS NULL AND available_at <= $1
 ORDER BY available_at, id
 LIMIT 100
 FOR UPDATE SKIP LOCKED;
-- Publish with broker confirmation, then:
UPDATE game.outbox SET published_at = $1 WHERE id = ANY($2);
```

## game

```sql
CREATE TABLE game.players (
    player_id     uuid        PRIMARY KEY,   -- equals the sub claim from auth
    display_name  text        NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 32),
    status        text        NOT NULL DEFAULT 'active'
                              CHECK (status IN ('active', 'deleting')),
    created_at    timestamptz NOT NULL
);

-- v1: one idol per player; the idol's design is still an open question.
CREATE TABLE game.idols (
    idol_id          uuid        PRIMARY KEY,
    player_id        uuid        NOT NULL UNIQUE
                                 REFERENCES game.players (player_id) ON DELETE CASCADE,
    name             text        NOT NULL,
    age              smallint    NOT NULL CHECK (age BETWEEN 20 AND 29),
    appearance_tags  text[]      NOT NULL,
    personality      text        NOT NULL,
    created_at       timestamptz NOT NULL
);

CREATE TABLE game.seasons (
    season_id      uuid        PRIMARY KEY,
    name           text        NOT NULL,
    starts_at      timestamptz NOT NULL,
    ends_at        timestamptz NOT NULL,
    rules_version  integer     NOT NULL CHECK (rules_version >= 1),
    status         text        NOT NULL CHECK (status IN ('upcoming', 'active', 'closed')),
    CHECK (ends_at > starts_at),
    EXCLUDE USING gist (tstzrange(starts_at, ends_at) WITH &&)   -- seasons must not overlap
);
CREATE UNIQUE INDEX seasons_one_active_idx ON game.seasons ((true)) WHERE status = 'active';

CREATE TABLE game.destinations (
    destination_id      text         PRIMARY KEY,   -- a slug, for example id-jakarta-kota-tua
    name                text         NOT NULL,
    level               text         NOT NULL CHECK (level IN ('city', 'province', 'country')),
    parent_id           text         REFERENCES game.destinations (destination_id),
    country_code        char(2)      NOT NULL,
    region_code         text         NOT NULL,
    latitude            numeric(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude           numeric(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    timezone            text         NOT NULL,      -- IANA, for lighting that matches local time
    follower_threshold  bigint       NOT NULL CHECK (follower_threshold >= 0),
    description         text         NOT NULL,
    active              boolean      NOT NULL DEFAULT true
);

CREATE TABLE game.destination_photos (
    photo_id            uuid     PRIMARY KEY,
    destination_id      text     NOT NULL REFERENCES game.destinations (destination_id),
    mapillary_image_id  text     NOT NULL UNIQUE,
    s3_key              text     NOT NULL,          -- assets/destinations/...
    photographer        text     NOT NULL,
    license             text     NOT NULL DEFAULT 'CC BY-SA 4.0',
    anchor_foot_x       real     NOT NULL CHECK (anchor_foot_x BETWEEN 0 AND 1),
    anchor_foot_y       real     NOT NULL CHECK (anchor_foot_y BETWEEN 0 AND 1),
    anchor_scale        real     NOT NULL CHECK (anchor_scale > 0),
    light_azimuth_deg   smallint NOT NULL CHECK (light_azimuth_deg BETWEEN 0 AND 359),
    active              boolean  NOT NULL DEFAULT true
);
CREATE INDEX destination_photos_destination_idx ON game.destination_photos (destination_id)
    WHERE active;

CREATE TABLE game.catalog_items (
    item_id               text    PRIMARY KEY,      -- for example casual-denim
    kind                  text    NOT NULL CHECK (kind IN ('outfit', 'pose', 'expression')),
    name                  text    NOT NULL,
    style_tags            text[]  NOT NULL DEFAULT '{}',   -- for matching netizen interests
    requires_entitlement  boolean NOT NULL DEFAULT false,
    active                boolean NOT NULL DEFAULT true
);

-- A read copy from billing, filled from entitlement.granted and entitlement.revoked.
CREATE TABLE game.entitlements (
    player_id   uuid        NOT NULL REFERENCES game.players (player_id) ON DELETE CASCADE,
    item_id     text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('cosmetic', 'subscription', 'season_pass')),
    expires_at  timestamptz,
    PRIMARY KEY (player_id, item_id)
);

CREATE TABLE game.posts (
    post_id               uuid        PRIMARY KEY,   -- UUIDv5(NS_POST, player_id:idempotency_key)
    player_id             uuid        NOT NULL REFERENCES game.players (player_id) ON DELETE CASCADE,
    idol_id               uuid        NOT NULL REFERENCES game.idols (idol_id) ON DELETE CASCADE,
    idempotency_key       uuid        NOT NULL,
    request_hash          bytea       NOT NULL,
    destination_id        text        NOT NULL REFERENCES game.destinations (destination_id),
    photo_id              uuid        NOT NULL REFERENCES game.destination_photos (photo_id),
    outfit_id             text        NOT NULL REFERENCES game.catalog_items (item_id),
    pose_id               text        NOT NULL REFERENCES game.catalog_items (item_id),
    expression_id         text        NOT NULL REFERENCES game.catalog_items (item_id),
    caption               text        NOT NULL CHECK (char_length(caption) <= 280),
    express               boolean     NOT NULL,
    credit_cost           bigint      NOT NULL CHECK (credit_cost >= 0),
    status                text        NOT NULL CHECK (status IN
                                      ('draft', 'queued', 'review', 'ready', 'rejected', 'failed')),
    failure_reason        text,
    date_wib              date        NOT NULL,
    created_at            timestamptz NOT NULL,
    requested_publish_at  timestamptz NOT NULL,
    ready_at              timestamptz,
    published_at          timestamptz,
    image_s3_key          text,
    image_tags            text[],
    rules_version         integer,
    frozen_inputs         jsonb,
    UNIQUE (player_id, idempotency_key),
    CHECK (status <> 'ready' OR (published_at IS NOT NULL
                                 AND image_s3_key IS NOT NULL
                                 AND frozen_inputs IS NOT NULL))
);
CREATE INDEX posts_player_created_idx ON game.posts (player_id, created_at DESC);
CREATE INDEX posts_daily_limit_idx ON game.posts (player_id, date_wib);
CREATE INDEX posts_feed_idx ON game.posts (player_id, published_at DESC)
    WHERE status = 'ready';
CREATE INDEX posts_destination_history_idx ON game.posts (player_id, destination_id, published_at DESC)
    WHERE status = 'ready';

CREATE TABLE game.popularity_events (
    id               bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_id        uuid        NOT NULL REFERENCES game.players (player_id) ON DELETE CASCADE,
    season_id        uuid        REFERENCES game.seasons (season_id),   -- NULL outside a season
    post_id          uuid        REFERENCES game.posts (post_id) ON DELETE CASCADE,
    kind             text        NOT NULL CHECK (kind IN ('engagement', 'adjustment')),
    effective_at     timestamptz NOT NULL,
    likes            integer     NOT NULL DEFAULT 0 CHECK (likes >= 0),
    comments         integer     NOT NULL DEFAULT 0 CHECK (comments >= 0),
    delta_followers  bigint      NOT NULL,
    delta_score      bigint      NOT NULL CHECK (delta_score >= 0),
    rules_version    integer     NOT NULL,
    source_event_id  uuid        NOT NULL,
    recorded_at      timestamptz NOT NULL,
    folded_at        timestamptz,
    UNIQUE (source_event_id, effective_at)
);
CREATE INDEX popularity_events_player_idx ON game.popularity_events (player_id, effective_at);
CREATE INDEX popularity_events_post_idx ON game.popularity_events (post_id, effective_at);
CREATE INDEX popularity_events_unfolded_idx ON game.popularity_events (effective_at)
    WHERE folded_at IS NULL;

-- Created together with the player, so the fold only needs an UPDATE.
CREATE TABLE game.follower_snapshots (
    player_id  uuid        PRIMARY KEY REFERENCES game.players (player_id) ON DELETE CASCADE,
    followers  bigint      NOT NULL CHECK (followers >= 0),
    as_of      timestamptz NOT NULL
);

CREATE TABLE game.season_scores (
    season_id  uuid        NOT NULL REFERENCES game.seasons (season_id),
    player_id  uuid        NOT NULL REFERENCES game.players (player_id) ON DELETE CASCADE,
    score      bigint      NOT NULL CHECK (score >= 0),
    as_of      timestamptz NOT NULL,
    PRIMARY KEY (season_id, player_id)
);

CREATE TABLE game.leaderboard_final (
    season_id     uuid    NOT NULL REFERENCES game.seasons (season_id),
    rank          integer NOT NULL CHECK (rank >= 1),
    player_id     uuid,            -- NULL once the player's account is deleted
    display_name  text    NOT NULL,
    score         bigint  NOT NULL CHECK (score >= 0),
    PRIMARY KEY (season_id, rank)
);

-- No foreign key: the player row is deleted before the request finishes.
CREATE TABLE game.deletion_requests (
    request_id     uuid        PRIMARY KEY,
    player_id      uuid        NOT NULL,
    kind           text        NOT NULL CHECK (kind IN ('delete', 'export')),
    status         text        NOT NULL CHECK (status IN ('pending', 'completed', 'stuck')),
    services_done  text[]      NOT NULL DEFAULT '{}',
    requested_at   timestamptz NOT NULL,
    completed_at   timestamptz
);
CREATE INDEX deletion_requests_open_idx ON game.deletion_requests (requested_at)
    WHERE status = 'pending';
```

## billing

The ledger uses an internal `account_id`, not `player_id`. When a player's account is deleted, `accounts.player_id` is cleared, so the ledger entries stay intact but are no longer linked to the person.

```sql
CREATE TABLE billing.accounts (
    account_id  uuid        PRIMARY KEY,
    player_id   uuid        UNIQUE,            -- NULL once the player's account is deleted
    available   bigint      NOT NULL DEFAULT 0 CHECK (available >= 0),
    held        bigint      NOT NULL DEFAULT 0 CHECK (held >= 0),
    debt        bigint      NOT NULL DEFAULT 0 CHECK (debt >= 0),
    created_at  timestamptz NOT NULL,
    closed_at   timestamptz
);

CREATE TABLE billing.ledger_entries (
    entry_id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id       uuid        NOT NULL REFERENCES billing.accounts (account_id),
    entry_type       text        NOT NULL CHECK (entry_type IN
                                 ('topup', 'ad_reward', 'daily_reward', 'hold', 'capture',
                                  'void', 'purchase', 'refund', 'adjustment')),
    delta_available  bigint      NOT NULL,
    delta_held       bigint      NOT NULL DEFAULT 0,
    delta_debt       bigint      NOT NULL DEFAULT 0,
    source_type      text        NOT NULL,   -- hold, payment, ad, daily, purchase, admin
    source_id        text        NOT NULL,   -- for example <hold_id>:<generation>
    reason           text,
    created_by       uuid,                   -- the admin, for adjustments only
    created_at       timestamptz NOT NULL,
    UNIQUE (source_type, source_id, entry_type),
    CHECK (entry_type <> 'adjustment' OR (reason IS NOT NULL AND created_by IS NOT NULL))
);
CREATE INDEX ledger_entries_account_idx ON billing.ledger_entries (account_id, entry_id);

CREATE FUNCTION billing.reject_ledger_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'billing.ledger_entries is append-only';
END;
$$;

CREATE TRIGGER ledger_entries_append_only
    BEFORE UPDATE OR DELETE ON billing.ledger_entries
    FOR EACH ROW EXECUTE FUNCTION billing.reject_ledger_change();

CREATE TABLE billing.holds (
    hold_id      uuid        PRIMARY KEY,
    account_id   uuid        NOT NULL REFERENCES billing.accounts (account_id),
    amount       bigint      NOT NULL CHECK (amount > 0),
    ref_type     text        NOT NULL CHECK (ref_type IN ('post', 'reroll')),
    ref_id       uuid        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('pending', 'attached', 'captured', 'voided')),
    generation   integer     NOT NULL DEFAULT 1 CHECK (generation >= 1),
    void_reason  text,
    created_at   timestamptz NOT NULL,
    attached_at  timestamptz,
    closed_at    timestamptz
);
CREATE INDEX holds_pending_idx ON billing.holds (created_at) WHERE status = 'pending';
CREATE INDEX holds_attached_idx ON billing.holds (attached_at) WHERE status = 'attached';

CREATE TABLE billing.payments (
    payment_id       uuid        PRIMARY KEY,
    account_id       uuid        NOT NULL REFERENCES billing.accounts (account_id),
    provider         text        NOT NULL CHECK (provider IN ('google_play', 'doku')),
    provider_ref     text        NOT NULL,   -- a Google Play purchase token or a DOKU invoice number
    product_id       text        NOT NULL,
    amount_idr       bigint      NOT NULL CHECK (amount_idr >= 0),
    credits          bigint      NOT NULL CHECK (credits >= 0),
    status           text        NOT NULL CHECK (status IN ('pending', 'paid', 'failed', 'refunded')),
    acknowledged_at  timestamptz,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    UNIQUE (provider, provider_ref)
);
CREATE INDEX payments_pending_idx ON billing.payments (created_at) WHERE status = 'pending';

CREATE TABLE billing.entitlements (
    account_id  uuid        NOT NULL REFERENCES billing.accounts (account_id),
    item_id     text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('cosmetic', 'subscription', 'season_pass')),
    source      text        NOT NULL CHECK (source IN ('purchase', 'play_subscription', 'reward')),
    granted_at  timestamptz NOT NULL,
    expires_at  timestamptz,
    revoked_at  timestamptz,
    PRIMARY KEY (account_id, item_id)
);

-- The cost ledger, from usage.recorded. No player_id: post_id is enough for the cost per post.
CREATE TABLE billing.usage_costs (
    usage_id        uuid          PRIMARY KEY,
    provider        text          NOT NULL CHECK (provider IN ('cloudflare', 'modal', 'xai')),
    model           text          NOT NULL,
    post_id         uuid,
    input_tokens    bigint        NOT NULL DEFAULT 0,
    output_tokens   bigint        NOT NULL DEFAULT 0,
    neurons         bigint        NOT NULL DEFAULT 0,
    gpu_seconds     numeric(12,3) NOT NULL DEFAULT 0,
    cost_micro_usd  bigint        NOT NULL CHECK (cost_micro_usd >= 0),
    occurred_at     timestamptz   NOT NULL,
    date_utc        date          NOT NULL    -- the Workers AI allowance resets at 00:00 UTC
);
CREATE INDEX usage_costs_daily_idx ON billing.usage_costs (provider, date_utc);
```

```sql
-- example: HoldCredit, idempotent per hold_id
BEGIN;
SELECT account_id, available FROM billing.accounts WHERE player_id = $1 FOR UPDATE;
-- The balance is checked here for a clear error message; CHECK (available >= 0) stays the last line of defense.
INSERT INTO billing.holds (hold_id, account_id, amount, ref_type, ref_id, status, created_at)
VALUES ($2, $3, $4, 'post', $2, 'pending', $5)
ON CONFLICT (hold_id) DO NOTHING;
-- If no row was inserted, the hold already exists: return it as is without touching the balance.
UPDATE billing.accounts SET available = available - $4, held = held + $4
 WHERE account_id = $3;
INSERT INTO billing.ledger_entries (account_id, entry_type, delta_available, delta_held,
                                    source_type, source_id, created_at)
VALUES ($3, 'hold', -$4, $4, 'hold', $2 || ':1', $5);
COMMIT;
```

```sql
-- example: capture when image.completed arrives, including when post.submitted hasn't been processed yet
UPDATE billing.holds
   SET status = 'captured', closed_at = $2
 WHERE hold_id = $1 AND status IN ('pending', 'attached')
RETURNING account_id, amount, generation;
-- If a row comes back: reduce held, then write a capture entry with source_id = hold_id:generation.
```

## imaging

```sql
-- What an item means for gameplay belongs to game; how it is rendered belongs to imaging.
CREATE TABLE imaging.render_catalog (
    item_id        text   PRIMARY KEY,   -- equals game.catalog_items.item_id
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
    job_id          uuid          PRIMARY KEY,   -- UUIDv5(NS_JOB, post_id:variant)
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

CREATE TABLE imaging.review_items (
    job_id      uuid        PRIMARY KEY REFERENCES imaging.image_jobs (job_id) ON DELETE CASCADE,
    s3_key      text        NOT NULL,   -- review/{job_id}.webp
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
```

## channels

```sql
CREATE TABLE channels.device_tokens (
    player_id     uuid        NOT NULL,
    fcm_token     text        NOT NULL,
    platform      text        NOT NULL CHECK (platform IN ('android')),
    created_at    timestamptz NOT NULL,
    last_seen_at  timestamptz NOT NULL,
    PRIMARY KEY (player_id, fcm_token)
);

CREATE TABLE channels.scheduled_pushes (
    push_id     uuid        PRIMARY KEY,
    player_id   uuid        NOT NULL,
    post_id     uuid,
    kind        text        NOT NULL CHECK (kind IN
                            ('photo_ready', 'first_comment', 'engagement_summary')),
    send_at     timestamptz NOT NULL,   -- already moved out of the 22:00-07:00 WIB quiet hours
    sent_at     timestamptz,
    attempts    integer     NOT NULL DEFAULT 0,
    last_error  text,
    UNIQUE (player_id, post_id, kind)
);
CREATE INDEX scheduled_pushes_due_idx ON channels.scheduled_pushes (send_at)
    WHERE sent_at IS NULL;
CREATE INDEX scheduled_pushes_player_idx ON channels.scheduled_pushes (player_id);
```

## ops

```sql
CREATE TABLE ops.dead_letters (
    dead_letter_id  uuid        PRIMARY KEY,
    dlq_topic       text        NOT NULL,
    dlq_partition   integer     NOT NULL,
    dlq_offset      bigint      NOT NULL,
    source_topic    text        NOT NULL,
    consumer        text        NOT NULL,   -- from the x-consumer header
    event_id        uuid,                   -- from the ce_id header
    event_type      text,
    message_key     text,
    headers         jsonb       NOT NULL,
    payload         bytea       NOT NULL,
    error           text        NOT NULL,   -- from the x-error header
    received_at     timestamptz NOT NULL,
    expires_at      timestamptz NOT NULL,   -- received_at + 14 days
    replayed_at     timestamptz,
    replayed_by     uuid,
    UNIQUE (dlq_topic, dlq_partition, dlq_offset)
);
CREATE INDEX dead_letters_received_idx ON ops.dead_letters (source_topic, received_at DESC);
CREATE INDEX dead_letters_expiry_idx ON ops.dead_letters (expires_at);
```

## analytics

```sql
-- Deleting this row means none of the player's analytics data can be linked back to the person.
CREATE TABLE analytics.pseudonym_keys (
    player_id       uuid        PRIMARY KEY,
    key_ciphertext  text        NOT NULL,   -- a 32-byte HMAC key, encrypted with Vault Transit
    created_at      timestamptz NOT NULL
);
```

## Retention

Aurora's 1 GiB storage limit makes routine cleanup mandatory ([design §14](../system-design.md#14-data-storage)). Every deletion runs in batches of 5,000 rows so it never holds locks for long.

| Table | Rule | Schedule |
|---|---|---|
| `*.outbox` | Delete rows whose `published_at` is more than 3 days ago | Hourly |
| `*.processed_events` | Delete rows whose `processed_at` is more than 30 days ago | Daily |
| `ops.dead_letters` | Delete rows whose `expires_at` has passed | Daily |
| `imaging.review_items` | Delete items decided more than 30 days ago | Daily |
| `game.popularity_events` | Once the season is `closed` and every event is folded: export to S3 (Parquet) and BigQuery, then delete | Once per season |
| `game.deletion_requests` | Delete requests completed more than 30 days ago | Daily |
