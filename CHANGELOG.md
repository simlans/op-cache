# Changelog

## 0.1.0 (2026-06-05)


### Features

* add --account flag for multi-account support ([1080c5f](https://github.com/simlans/op-cache/commit/1080c5fd1a5d0267658a4b8b49057397c68f438b))
* initial implementation of op-cache ([9bce876](https://github.com/simlans/op-cache/commit/9bce8764b8efe7f89bab39730a76d3c2a9634edf))
* **run:** add `run` subcommand to exec with resolved secrets ([5ab1bd6](https://github.com/simlans/op-cache/commit/5ab1bd64b9259c1f709727fac84a1fe176a1c7df))


### Bug Fixes

* **daemon:** secure unix socket permissions on bind ([5aea6d0](https://github.com/simlans/op-cache/commit/5aea6d0d98e43c0bfd9a838a2da0b5ce90b9b3b0))

## 0.2.0

### Added
- `--account` flag on `read` and `run` subcommands to target a specific 1Password account.
- Ambient `OP_ACCOUNT` environment variable is now included in cache key computation, preventing cross-account cache collisions.

### Fixed
- Cache entries are now partitioned by effective account (explicit `--account` or `OP_ACCOUNT` env var), fixing a bug where the same reference on different accounts could return a stale secret from the wrong account.

## 0.1.0

- Initial release: daemon-based caching proxy for `op read`, with `read`, `run`, `status`, `stats`, `clear`, and `stop` subcommands.
