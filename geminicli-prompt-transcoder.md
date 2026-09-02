# Prompt — stream-transcoder-service (Spring Boot)

Act as a senior Java backend engineer. Set up a production-grade Spring Boot project called **"stream-transcoder-service"** for a video transcoding pipeline.

## Build tool

Maven (`pom.xml`). Java 21.

## Architecture — layered, clean separation of concerns

`Controller -> Service -> Repository/DAO -> Mapper -> Entity/DTO`

- **Controller layer**: minimal, only if needed for health checks/actuator. This service is primarily a background worker (SQS consumer), not a REST-driven service.
- **Service layer**: business logic — chunk assembly orchestration, encoding orchestration.
- **Repository/DAO layer**: TWO separate repositories, since we have two distinct external systems:
  1. `TranscodeQueueRepository` (or `SqsMessageRepository`) — wraps SQS interactions (ReceiveMessage, DeleteMessage). Configured via `TRANSCODE_QUEUE_URL` env var.
  2. `VideoJobRepository` (Spring Data JPA repository) — wraps PostgreSQL persistence for video/job metadata. Configured via `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` env vars.
- **Mapper layer**: MapStruct (or manual mappers) to convert between SQS message DTOs, JPA entities, and internal domain models. Never leak JPA entities into the service logic directly — map to domain objects.

## Configuration

Externalize via environment variables (`application.yml` with env var placeholders, Spring profiles for dev/prod):

- `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` -> PostgreSQL datasource (Aurora PostgreSQL)
- `TRANSCODE_QUEUE_URL` -> SQS queue URL (not a topic — SQS uses queue URLs, not ARNs, for send/receive operations)

Assume AWS credentials come from EKS Pod Identity (no hardcoded access keys, no static credentials provider). Use AWS SDK for Java v2 (`software.amazon.awssdk`) for both S3 and SQS clients.

## Concurrency model — Java 21 Virtual Threads

Use Java 21 virtual threads for all I/O-bound concurrent work (chunk downloads, segment uploads) instead of traditional fixed thread pools:

- `Executors.newVirtualThreadPerTaskExecutor()` for parallel chunk downloads and parallel segment uploads.
- Do NOT use platform threads with a manually tuned pool size for I/O-bound work — virtual threads are the idiomatic Java 21 approach here since the work is I/O-bound (network calls to S3), not CPU-bound.
- FFmpeg execution itself (via `ProcessBuilder`) remains a separate concern — it's an external process, not a JVM thread, so virtual threads don't apply there; but the thread that waits on/monitors the FFmpeg process (and its `WatchService`) can itself run on a virtual thread.
- Spring Boot 3.2+ supports virtual threads for `@Async` and task executors natively (`spring.threads.virtual.enabled=true`) — enable this where applicable.

## SQS message contract

Incoming message body (JSON):

```json
{
  "video_id": "string",
  "chunks": [ { "chunk_index": 0, "s3_url": "string" } ],
  "target_resolutions": ["1080p", "720p", "480p", "360p"]
}
```

(chunks are already sorted by `chunk_index`)

## Design patterns required

### 1. Registry Pattern for resolutions

Implement a `ResolutionRegistry` that maps resolution keys (e.g. `"1080p"`) to their encoding profile (width, height, bitrate, etc.). It must be easy to register new resolutions without modifying existing code (open/closed principle) — e.g. a `ResolutionProfile` record/class registered at startup (via Spring bean configuration or a static registry populated at init), not a hardcoded switch/enum with logic baked in.

Example shape:

- `ResolutionProfile { name, width, height, videoBitrate, audioBitrate }`
- `ResolutionRegistry.register(ResolutionProfile)`
- `ResolutionRegistry.get(String name) -> ResolutionProfile`
- `ResolutionRegistry.getAll() -> Collection<ResolutionProfile>`

Initial registrations: `1080p`, `720p`, `480p`, `360p` — but the registry must support adding more later (e.g. `1440p`, `240p`) with zero changes to the encoding logic.

### 2. Factory Pattern for encoders

Implement an `EncoderFactory` that returns the correct `VideoEncoder` implementation based on the target streaming format (HLS now, DASH later).

- Interface: `VideoEncoder { void encode(File assembledVideo, ResolutionProfile profile, String outputPath); }`
- Concrete implementation now: `HlsEncoder implements VideoEncoder` — wraps FFmpeg via `ProcessBuilder`, using `-hls_time <seconds>` (configurable, e.g. 6s segments) and `-hls_segment_type` (mpegts or fmp4, configurable) to produce a rendition playlist (`.m3u8`) + time-based segments per resolution.
- `EncoderFactory.getEncoder(EncodingFormat format)` returns `HlsEncoder` for `EncodingFormat.HLS`. Structure it so adding `DashEncoder` later only requires a new class + one factory registration — no changes to service/orchestration logic.

## Processing flow (SQS consumer)

1. `@SqsListener` on the queue URL from `TRANSCODE_QUEUE_URL`, long polling.
2. Parse message body into a `TranscodeJobMessage` DTO.

3. **Step 1 — Chunk download (parallel, virtual threads)**
   - Create a local working directory: `/tmp/transcode/{video_id}/chunks/`.
   - Download all chunks from their `s3_url` in parallel using `Executors.newVirtualThreadPerTaskExecutor()` — one virtual thread task per chunk.
   - Each chunk downloaded via the S3 client (`GetObject`) to a local file named to preserve order (e.g. `chunk_{chunk_index}.part`).
   - Wait for all downloads to complete (`CompletableFuture.allOf` or `ExecutorService.invokeAll`) before proceeding; fail the job fast if any chunk download fails.

4. **Step 2 — Sequential local assembly**
   - Merge downloaded chunks **sequentially in `chunk_index` order** into a single file using `FileOutputStream` + buffered streaming copy (do not load full chunks into memory — stream in fixed-size buffers).
   - Output: `/tmp/transcode/{video_id}/assembled.mp4`.
   - Delete the individual chunk files from disk immediately after successful assembly.

5. **Step 3 — Encoding with progressive upload (HLS)**
   - For each resolution in `target_resolutions`:
     - Fetch the `ResolutionProfile` from `ResolutionRegistry`.
     - Get the `VideoEncoder` from `EncoderFactory` for `EncodingFormat.HLS`.
     - Start FFmpeg on `assembled.mp4` as a background process (`ProcessBuilder`), outputting time-based segments to `/tmp/transcode/{video_id}/hls/{resolution}/`.
     - Concurrently, use `WatchService` to monitor that output directory: as soon as a new `.ts`/`.m4s` segment file is fully written (closed), immediately upload it to S3 (target bucket, key prefix `{video_id}/hls/{resolution}/`) on a virtual thread — do NOT wait for FFmpeg to finish the entire resolution before starting uploads.
     - Once FFmpeg finishes and the rendition `.m3u8` playlist is written, upload it last (it references segments that must already exist in S3).
   - After all renditions are encoded and uploaded, generate and upload a master `.m3u8` playlist referencing all rendition playlists.

6. **Step 4 — Cleanup (always runs)**
   - Delete the entire local working directory `/tmp/transcode/{video_id}/` (chunks, assembled.mp4, hls output) — wrap in try/finally so cleanup runs whether the job succeeds or fails, avoiding orphaned files on pod ephemeral storage.
   - At the **start** of processing, also proactively clean/recreate `/tmp/transcode/{video_id}/` in case a previous crashed attempt left partial files (idempotency on retry/redelivery).

7. Persist job status/metadata to PostgreSQL via `VideoJobRepository` (status transitions: `RECEIVED -> DOWNLOADING -> ASSEMBLING -> ENCODING -> UPLOADING -> COMPLETED/FAILED`). Update status at each step transition, not just at the end.

8. On success, delete the SQS message. On failure, log with full context (video_id, failed step, exception) and do NOT delete the message — let it return to the queue after the visibility timeout for retry, eventually landing in the DLQ via redrive policy.

## Non-functional requirements

- Proper exception handling — a single job failure must not crash the listener thread.
- SQS visibility timeout heartbeat must cover the **entire** flow (download + assembly + encoding + progressive upload), not just encoding — extend visibility periodically via a scheduled heartbeat while the job is in progress.
- Structured logging (video_id, stage, resolution where applicable) for observability.
- Make the HLS segment duration configurable (e.g. `transcode.hls.segment-duration-seconds`, default 6).
- Disk space awareness: assume pod has limited ephemeral storage (`emptyDir` with `sizeLimit`) — cleanup must be reliable, and log a warning if available disk space is low before starting a job.
- Clean package structure: `controller/` (if any), `service/`, `repository/`, `mapper/`, `dto/`, `entity/`, `encoder/` (factory + implementations), `resolution/` (registry + profiles), `storage/` (S3 download/upload helpers + WatchService-based progressive uploader), `config/`.

## Deliverable

Generate the full Maven project structure (`pom.xml`), with required dependencies (Spring Boot 3.2+, Spring Cloud AWS SQS/S3 starter, Spring Data JPA, PostgreSQL driver, MapStruct, Lombok), and the code for each layer described above.
