# Vendored `url` 2.5.8

## Upstream provenance

- Crate: [`url`](https://crates.io/crates/url/2.5.8) 2.5.8
- Repository: <https://github.com/servo/rust-url>
- Upstream Git commit recorded by the crate: `d6ea13c5f8e7e6e627f6390161b3e185bda5e5ce`
- crates.io archive SHA-256: `ff67a8a4397373c3ef660812acab3268222035010ab8680ec4215f38ba3d0eed`
- License: `MIT OR Apache-2.0`; the unmodified license texts are retained as
  `LICENSE-MIT` and `LICENSE-APACHE`.
- Source: the complete 20-file crate directory was copied from Cargo's local registry cache.

## Local patch

The optional `go-path-segment-compat` feature changes one decision in
`src/parser.rs::Parser::parse_path`:

- `Url::parse`/`Url::options().parse` with **no base URL** preserves complete `.` and `..`
  path segments, including the percent-encoded forms that upstream recognizes as dot segments.
- `Url::set_path` preserves the same segments.
- `Url::join`, parsing through `ParseOptions::base_url(Some(_))`, and redirect-relative URL
  resolution continue to use the upstream WHATWG dot-segment normalization.
- `PathSegmentsMut` continues to use its upstream `PathSegmentSetter` behavior.
- Builds without the feature are behaviorally identical to upstream 2.5.8.

This narrow deviation is required because Go's `url.PathEscape` leaves a dynamic path segment
whose complete value is `.` or `..` unchanged. The original Go client therefore sends those bytes
in the HTTP request-target, whereas upstream `url` removes them while parsing the absolute URL.
The feature is explicitly enabled only by the root `environment-manager` package.

## Maintenance

When updating `url`, re-copy the full crate, update the version, Git commit, archive checksum and
licenses above, re-apply the single guarded parser change, and run the Linux ARM64 tests in
`tests/url_dot_segment_compat.rs` together with the project's locked test suite.
