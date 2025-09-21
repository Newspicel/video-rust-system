# Repository Guidelines

## Project Structure & Module Organization
- `src/` holds Rust application code; `src/main.rs` currently exposes the CLI entry point via `main()`. Add new modules with `mod` declarations at the top of `main.rs` or factor them into `src/<feature>.rs` and reference them in `lib.rs` when introduced.
- `target/` is Cargo’s build output; do not commit it. Create a `tests/` directory for integration tests when the surface expands.
- Keep configuration (e.g., `.cargo/`, `configs/`) grouped by concern; document new directories in this guide.
- `data/libs/` caches external executables fetched at runtime (e.g., `yt-dlp`, `ffmpeg`); it is created automatically by the server when needed.

## Build, Test, and Development Commands
- `cargo run` – compile in debug mode and execute the binary for quick feedback.
- `cargo build --release` – emit an optimized binary under `target/release/` for benchmarking or deployment.
- `cargo fmt` – run before every commit; CI enforces formatting via `cargo fmt -- --check`.
- `cargo clippy --all-targets --all-features -- -D warnings` – keep the codebase lint-clean; CI treats all warnings as errors.
- `cargo test --lib` & `cargo test --test api` – unit suites that must pass locally before pushing; GitHub Actions executes both on every push/PR.
- `cargo test` – convenience wrapper that runs the entire test suite (unit + API).

### Continuous Integration
- GitHub Actions workflow lives in `.github/workflows/ci.yml`; it runs `cargo fmt --check`, full clippy, and the unit/API tests on each push and pull request.

## Coding Style & Naming Conventions
- Follow Rust 2021 edition defaults: 4-space indentation, snake_case for functions/variables, UpperCamelCase for types, SCREAMING_SNAKE_CASE for constants.
- Prefer `?` for error propagation and explicit `use` statements at module heads. Keep modules under ~300 lines; split when logic grows.
- Run `cargo fmt` prior to commits to maintain canonical formatting.

## Testing Guidelines
- Co-locate fast unit tests in the same file with a `#[cfg(test)] mod tests` block. Name tests `fn <subject>_<condition>_<expected>()`.
- Place end-to-end checks in `tests/` once created; arrange fixtures under `tests/fixtures/` and gate external resources behind feature flags.
- Aim for meaningful branch coverage on critical flows; add regression tests for every bug fix.

## Commit & Pull Request Guidelines
- Use Conventional Commits (`feat:`, `fix:`, `chore:`, etc.) to clarify intent, e.g., `feat: add geometry solver module`.
- Reference tracking issues with `Closes #<id>` in the PR body. Include reproduction steps or screenshots for user-facing changes.
- Run `cargo fmt`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --lib && cargo test --test api` locally before opening a PR; CI enforces this pipeline.
