-- The game schema, matching docs/detail/postgres-schema.md.

CREATE TABLE game.outbox (
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
CREATE INDEX outbox_due_idx ON game.outbox (available_at, id) WHERE published_at IS NULL;
CREATE INDEX outbox_cleanup_idx ON game.outbox (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX outbox_player_pending_idx ON game.outbox (player_id) WHERE published_at IS NULL;

CREATE TABLE game.processed_events (
    consumer      text        NOT NULL,
    event_id      uuid        NOT NULL,
    processed_at  timestamptz NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_cleanup_idx ON game.processed_events (processed_at);

CREATE TABLE game.players (
    player_id     uuid        PRIMARY KEY,
    display_name  text        NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 32),
    status        text        NOT NULL DEFAULT 'active'
                              CHECK (status IN ('active', 'deleting')),
    created_at    timestamptz NOT NULL
);

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
    EXCLUDE USING gist (tstzrange(starts_at, ends_at) WITH &&)
);
CREATE UNIQUE INDEX seasons_one_active_idx ON game.seasons ((true)) WHERE status = 'active';

CREATE TABLE game.destinations (
    destination_id      text         PRIMARY KEY,
    name                text         NOT NULL,
    level               text         NOT NULL CHECK (level IN ('city', 'province', 'country')),
    parent_id           text         REFERENCES game.destinations (destination_id),
    country_code        char(2)      NOT NULL,
    region_code         text         NOT NULL,
    latitude            numeric(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude           numeric(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    timezone            text         NOT NULL,
    follower_threshold  bigint       NOT NULL CHECK (follower_threshold >= 0),
    description         text         NOT NULL,
    active              boolean      NOT NULL DEFAULT true
);

CREATE TABLE game.destination_photos (
    photo_id            uuid     PRIMARY KEY,
    destination_id      text     NOT NULL REFERENCES game.destinations (destination_id),
    mapillary_image_id  text     NOT NULL UNIQUE,
    s3_key              text     NOT NULL,
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
    item_id               text    PRIMARY KEY,
    kind                  text    NOT NULL CHECK (kind IN ('outfit', 'pose', 'expression')),
    name                  text    NOT NULL,
    style_tags            text[]  NOT NULL DEFAULT '{}',
    requires_entitlement  boolean NOT NULL DEFAULT false,
    active                boolean NOT NULL DEFAULT true
);

CREATE TABLE game.entitlements (
    player_id   uuid        NOT NULL REFERENCES game.players (player_id) ON DELETE CASCADE,
    item_id     text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('cosmetic', 'subscription', 'season_pass')),
    expires_at  timestamptz,
    PRIMARY KEY (player_id, item_id)
);

CREATE TABLE game.posts (
    post_id               uuid        PRIMARY KEY,
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
    season_id        uuid        REFERENCES game.seasons (season_id),
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
    player_id     uuid,
    display_name  text    NOT NULL,
    score         bigint  NOT NULL CHECK (score >= 0),
    PRIMARY KEY (season_id, rank)
);

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
