# VRS

VRS (Video Rust Service) ingests video from several sources, transcodes it to modern AV1/Opus outputs, and makes the resulting assets available for direct download, HLS, and DASH playback. The service is built with [Axum](https://github.com/tokio-rs/axum) and runs entirely on Tokio, making it suitable for self-hosted deployments or as the backend for browser front-ends.

## Highlights

- **Multiple ingest paths** – accept direct file uploads, fetch HTTP(S) URLs, download torrents/magnets via `aria2c`, or hand off to `yt-dlp` for site-specific extractors.
- **Tracked job pipeline** – every ingest request receives a job identifier with progress, stage, ETA, and error reporting exposed at `GET /jobs/{id}`.
- **Adaptive transcoding** – AV1 encoding (VideoToolbox, NVENC, QSV, VA-API, or libaom) with per-request control over `crf`/`cpu_used`, plus automatic fallback when hardware acceleration is unavailable.
- **Streaming-friendly outputs** – finalized assets include a range-enabled WebM download as well as HLS (`master.m3u8`) and MPEG-DASH (`manifest.mpd`) ladders generated from the encoded source.
- **Storage-aware housekeeping** – periodic cleanup keeps temporary HLS/DASH outputs trimmed according to minimum free-space thresholds.

## Prerequisites

| Tool | Purpose | Notes |
| ---- | ------- | ----- |
| `cargo` / `rustc` | Build and run the service | Requires a Rust toolchain with the 2024 edition (Rust 1.82+ recommended). |
| `ffmpeg` | Transcoding and segment generation | Must be discoverable on `$PATH`. |
| `yt-dlp` | Site-specific video extraction | Optional unless `/download/yt-dlp` is exercised. |
| `aria2c` | High-throughput remote downloads, torrents, magnets | Optional unless torrent/magnet ingestion is used. |

Ensure external binaries are executable by the same user that runs the VRS process.

## Quick Start

1. Install the prerequisites listed above.
2. Pick a storage directory (defaults to `./data`). Ensure the process has read/write access.
3. Run the server:

   ```bash
   cargo run
   ```

   By default the service listens on `0.0.0.0:3000`.

4. Ingest a video via a remote URL:

   ```bash
   curl -X POST http://localhost:3000/upload/remote \
     -H 'Content-Type: application/json' \
     -d '{"url":"https://example.com/video.mp4"}'
   ```

   The response includes the job id and the playback URLs:

   ```json
   {
     "id": "6f04e3e8-a8d2-4c4f-a5a9-5e6d9a4f2f35",
     "status_url": "/jobs/6f04e3e8-a8d2-4c4f-a5a9-5e6d9a4f2f35",
     "download_url": "/videos/6f04e3e8-a8d2-4c4f-a5a9-5e6d9a4f2f35/download",
     "hls_master_url": "/videos/6f04e3e8-a8d2-4c4f-a5a9-5e6d9a4f2f35/hls/master.m3u8",
     "dash_manifest_url": "/videos/6f04e3e8-a8d2-4c4f-a5a9-5e6d9a4f2f35/dash/manifest.mpd"
   }
   ```

## Configuration Reference

VRS relies on environment variables for runtime configuration.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `VIDEO_SERVER_ADDR` | `0.0.0.0:3000` | Socket address to bind for the HTTP service. |
| `VIDEO_STORAGE_DIR` | `data` | Root directory for persisted encodes, e.g. `/srv/vrs`. Each video lives inside `<VIDEO_STORAGE_DIR>/<uuid>/`. |
| `VIDEO_SERVER_ENCODER` | auto-detect | Force a particular encoder: `videotoolbox`, `nvenc`, `qsv`, `vaapi`, or `software`. |
| `VIDEO_VAAPI_DEVICE` | `/dev/dri/renderD128` | Override VA-API render node when `VIDEO_SERVER_ENCODER=vaapi`. |
| `VIDEO_STORAGE_MIN_FREE_BYTES` | `5368709120` (5 GiB) | Trigger cleanup when free space drops below this byte threshold. |
| `VIDEO_STORAGE_MIN_FREE_RATIO` | `0.1` | Trigger cleanup when free space is below this ratio of the total disk. |
| `VIDEO_STORAGE_CLEANUP_BATCH` | `5` | Maximum number of completed jobs to prune in a single cleanup pass. |
| `RUST_LOG` | `vrs=debug,axum=info,tower_http=info` | Standard tracing subscriber filter; adjust for quieter logs. |

Temporary working files (incoming uploads, generated segments) live under the system temp directory (e.g. `/tmp/vrs/`). The storage cleanup step removes stale HLS/DASH renditions once disk pressure exceeds configured thresholds.

## API Overview

All responses are JSON unless otherwise noted. Errors follow the shape `{ "error": "details" }` with appropriate HTTP status codes.

### `GET /healthz`
Simple readiness probe; returns `200 OK` with body `ok` plus permissive CORS headers.

### `POST /upload/multipart`
Accepts a `multipart/form-data` payload containing at least one file part. The first file is streamed to temporary storage, transcoded, and published. Returns the standard `UploadResponse` JSON payload shown above.

### `POST /upload/remote`
Fetches a file reachable via HTTP(S), FTP(S), or magnet/torrent link. Request body:

```json
{
  "url": "https://cdn.example.com/video.mp4",
  "transcode": {
    "crf": 28,
    "cpu_used": 6
  }
}
```

The optional `transcode` object lets clients override libaom `crf`/`cpu_used` values. Hardware-accelerated encoders ignore `cpu_used` but still honor `crf`.

### `POST /download/yt-dlp`
Delegates acquisition to `yt-dlp` for hosts that require custom extractors. Body schema matches `/upload/remote` but the `url` must be a valid HTTP(S) URL.

### `GET /jobs/{id}`
Returns the latest snapshot for a job:

```json
{
  "id": "6f04e3e8-a8d2-4c4f-a5a9-5e6d9a4f2f35",
  "stage": "transcoding",
  "progress": 0.73,
  "stage_progress": 0.42,
  "current_stage_index": 2,
  "total_stages": 3,
  "elapsed_seconds": 118.5,
  "estimated_remaining_seconds": 96.8,
  "error": null,
  "started_at_unix_ms": 1736965234123,
  "last_update_unix_ms": 1736965327881
}
```

Stages progress through `queued → uploading/downloading → transcoding → finalizing → complete`, with `failed` reported if an error occurs.

### Playback endpoints

- `GET /videos/{id}/download` (alias `/videos/{id}`) – Streams the WebM file; supports HTTP range requests.
- `GET /videos/{id}/hls/{*asset}` – Serves HLS playlists and segments (with automatic lazy generation if missing).
- `GET /videos/{id}/dash/{*asset}` – Serves DASH manifests and segments.

All playback endpoints require the associated job to have completed successfully.

## Storage Layout

```
VIDEO_STORAGE_DIR/
  └── <uuid>/
        └── download.webm     # AV1/Opus mezzanine
/tmp/vrs/
  ├── incoming/              # pending uploads and remote downloads
  ├── hls/<uuid>/            # generated HLS playlists + segments
  └── dash/<uuid>/           # generated DASH manifests + segments
```

The cleanup subsystem prunes HLS/DASH directories for completed jobs to reclaim disk space when thresholds are exceeded.

## Development Workflow

- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`
- Tests: `cargo test --lib` and `cargo test --test api`

`cargo test` runs the full suite (unit plus API). The integration tests spin up the router in-memory and validate the public endpoints.

## License

This project is distributed under the terms of the MIT License. See `LICENSE` for details.
