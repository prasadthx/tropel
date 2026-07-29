# Changelog

## [0.1.0] - 2026-07-29

### Added
- Initial project structure (Rust workspace with 14+ crates)
- Postman Collection v2.1/v2.0 parser (`tropel-collection`)
- `{{var}}` resolution with scope precedence (`tropel-variables`)
- QuickJS engine wrapper (`tropel-js`)
- Native Rust builtins for crypto, hashing, encoding, assertions (`tropel-native`)
- `pm.*` API bridge (`tropel-pm`)
- HTTP protocol executor with auth signers (`tropel-http`)
- VU scheduler with 4 execution modes (`tropel-executor`)
- HDR histogram metrics aggregation (`tropel-metrics`)
- Reporters: stdout, JSON, CSV (`tropel-report`)
- Extension SDK with registration system (`tropel-ext`)
- Engine orchestration facade (`tropel-engine`)
- CLI binary with `tropel run` command
- Vendored JS libraries: pm-api, chai, lodash, CryptoJS shim
- CI pipeline (format, lint, test, build)
- Extension crates: gRPC, WebSocket, Prometheus (placeholders)
- Custom binary builder (`tropel-build`)
