CREATE TABLE IF NOT EXISTS consumer_offsets (
    id SERIAL PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    current_offset BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(topic_name, consumer_group)
);