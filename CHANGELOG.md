## [1.3.2](https://github.com/asaidimu/blobs/compare/v1.3.1...v1.3.2) (2026-08-29)


### Bug Fixes

* fix dependencies ([79505a8](https://github.com/asaidimu/blobs/commit/79505a83902e24e5997026d6e2cf4eb2026fc60b))
* **index:** prevent scan operation on closed MemoryBackend ([f5a7c1e](https://github.com/asaidimu/blobs/commit/f5a7c1e51756bc6702891a05b080df13aa360237))
* **store:** add encryption-at-rest support for namespaces ([0dde046](https://github.com/asaidimu/blobs/commit/0dde04627babad03f8a44f8d8ec88f1bdeed96e5))

## [1.3.1](https://github.com/asaidimu/blobs/compare/v1.3.0...v1.3.1) (2026-08-07)


### Bug Fixes

* **staging:** support trailing partial blocks in aligned uploads ([fa7696c](https://github.com/asaidimu/blobs/commit/fa7696c41c3dcc50a8a55b621c4914ebfadd2174))

# [1.3.0](https://github.com/asaidimu/blobs/compare/v1.2.2...v1.3.0) (2026-08-06)


### Bug Fixes

* **chunking:** migrate to github.com/kalbasit/fastcdc ([a26c983](https://github.com/asaidimu/blobs/commit/a26c983ce8be313ced32ac100383888cd8ee1ee5))


### Features

* **volume:** implement content-defined chunking for blobs ([7389c56](https://github.com/asaidimu/blobs/commit/7389c56b6d012a9238815a36d9825b8373f3daf3)), closes [hi#performance](https://github.com/hi/issues/performance)

## [1.2.2](https://github.com/asaidimu/blobs/compare/v1.2.1...v1.2.2) (2026-08-04)


### Bug Fixes

* **store:** scope Compact phase 2 exclusivity to a single namespace ([ea15a9d](https://github.com/asaidimu/blobs/commit/ea15a9dcf67571f782eb586461706186d5be7f0e))


### Performance Improvements

* **index,store:** batch chunk lookups, cache blob lookups in compactPhase1 ([485c806](https://github.com/asaidimu/blobs/commit/485c80675906c6fe0a7caa2b0cbf1bd9c2e938c4))

## [1.2.1](https://github.com/asaidimu/blobs/compare/v1.2.0...v1.2.1) (2026-07-21)


### Bug Fixes

* Drop default namespaces ([e2b8d54](https://github.com/asaidimu/blobs/commit/e2b8d54bb4e66009544e59e6702e06d225035e99))

# [1.2.0](https://github.com/asaidimu/blobs/compare/v1.1.0...v1.2.0) (2026-07-21)


### Features

* add Update and Rename methods to NamespaceHandle ([5b79230](https://github.com/asaidimu/blobs/commit/5b79230c0727ebb7afd6ac5f3027a76f79b98638))

# [1.1.0](https://github.com/asaidimu/blobs/compare/v1.0.2...v1.1.0) (2026-07-20)


### Features

* store, volume: two-phase compaction with segment rewrite, WAL replay support ([dc09b6c](https://github.com/asaidimu/blobs/commit/dc09b6cefcfad9a0510d2a1bf73648c3f79287cc))

## [1.0.2](https://github.com/asaidimu/blobs/compare/v1.0.1...v1.0.2) (2026-07-15)


### Bug Fixes

* add ability to update blob metadata ([c44e9b4](https://github.com/asaidimu/blobs/commit/c44e9b4dc926422870428d80a5b2ed738d005bde))

## [1.0.1](https://github.com/asaidimu/blobs/compare/v1.0.0...v1.0.1) (2026-07-15)


### Bug Fixes

* implement some more features ([d543cc1](https://github.com/asaidimu/blobs/commit/d543cc1f32486760ec7b4959b0caabfe25bc111b))

# 1.0.0 (2026-07-15)


### Features

* **ci:** add deployment, testing, and versioning workflows ([432cd61](https://github.com/asaidimu/blobs/commit/432cd61a1fddcd40e115fc2f57684fa5c7829bbc))
