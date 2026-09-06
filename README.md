# JStream Broker

JStream is a lightweight, high-performance, reactive message broker built with **Spring Boot 3.2.4**, **Java 21**, 
and the **RSocket** protocol. It provides log-structured message persistence with automatic log retention and reliable, 
low-latency streaming to consumers with persistent offset tracking backed by a PostgreSQL database.

---

## Features

*   Uses Spring Webflux for maximum concurrency and optimal resource utilization.
*   Uses bi-directional RSocket over TCP (Port 7000) instead of HTTP polling, enabling high-performance push-based streaming.
*   Persists messages for each topic onto local disk partitions. Segments automatically roll over after a configurable 
    message limit (default 50).
*   Tracks consumer-group read offsets transactionally in PostgreSQL via reactive R2DBC drivers. 
*   If a consumer group reconnects, JStream automatically replays missed logs starting precisely from their last acknowledged 
    offset before piping live stream events.
*   A cron job runs every midnight to prune log segments older than 7 days, leaving active write channels untouched.