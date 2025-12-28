-- The content catalog (podcasts, videos, etc.)
CREATE TABLE content (
    id              UUID PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    content_type    TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds  INTEGER, 
    publish_ts      TIMESTAMPTZ NOT NULL
);

-- The raw stream of user engagement events
CREATE TABLE engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts     TIMESTAMPTZ NOT NULL,
    duration_ms  INTEGER,      -- nullable for events without duration
    device       TEXT,         -- e.g. "ios", "web‑safari"
    raw_payload  JSONB         -- anything extra the client sends
);

-- Enable full replica identity for CDC to work properly
ALTER TABLE engagement_events REPLICA IDENTITY FULL;
ALTER TABLE content REPLICA IDENTITY FULL;

-- Helper view to cast UUIDs to text for Flink compatibility
CREATE OR REPLACE VIEW content_dim_view AS 
SELECT 
    id::text, 
    slug, 
    title, 
    content_type, 
    length_seconds, 
    publish_ts 
FROM content;
