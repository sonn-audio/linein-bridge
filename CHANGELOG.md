# [2.0.0](https://github.com/sonn-audio/linein-bridge/compare/v1.10.1...v2.0.0) (2026-07-27)


* feat!: replace on_start/on_stop with a single on_command hook ([9583ff5](https://github.com/sonn-audio/linein-bridge/commit/9583ff52bc59b91439080cfc15b1470db48de8ed))


### Bug Fixes

* let the writer catch up instead of dropping audio ([555aecf](https://github.com/sonn-audio/linein-bridge/commit/555aecf4f3987c03cfec2dc456ec6a61c481085a))


### BREAKING CHANGES

* on_start and on_stop are no longer read. Replace them with a
single on_command script that switches on `$1` -- start and stop are passed as
commands where the old hooks fired.

The script text is left exactly as configured rather than having "$@" appended to
it, so a hook that already refers to its arguments does not silently receive them
twice.

## [1.10.1](https://github.com/sonn-audio/linein-bridge/compare/v1.10.0...v1.10.1) (2026-07-27)


### Bug Fixes

* pick the right server when several audioservers advertise ([39ed9df](https://github.com/sonn-audio/linein-bridge/commit/39ed9dfd00d000defe2a0673f49ff1506a33ece9))

# [1.10.0](https://github.com/sonn-audio/linein-bridge/compare/v1.9.1...v1.10.0) (2026-07-27)


### Bug Fixes

* browse the sonncore mDNS service type ([b939e1b](https://github.com/sonn-audio/linein-bridge/commit/b939e1b73beeec91e0a89142213a76c1ea7dd539))
* stop resampling against a mismeasured input rate ([930125e](https://github.com/sonn-audio/linein-bridge/commit/930125ef7b88572b12af0fdbcc59396faa253097))


### Features

* run on_start/on_stop hooks when the source is selected ([149af4f](https://github.com/sonn-audio/linein-bridge/commit/149af4f149dc70cac32d4461a8387f07c4d901b8))

## [1.9.1](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.9.0...v1.9.1) (2026-01-19)


### Bug Fixes

* match pacing interval to chunk size ([306bdd5](https://github.com/lox-audioserver/lox-linein-bridge/commit/306bdd5c2dffc2a54278a3d56d27c95e782fc0df))

# [1.9.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.8.1...v1.9.0) (2026-01-19)


### Features

* stabilize streaming timing with paced output and realtime tuning ([f5213b4](https://github.com/lox-audioserver/lox-linein-bridge/commit/f5213b4961e56026f006987e7ebbdb4152abd5ab))

## [1.8.1](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.8.0...v1.8.1) (2026-01-19)


### Bug Fixes

* log audio buffer underruns during paced streaming ([b53328e](https://github.com/lox-audioserver/lox-linein-bridge/commit/b53328e71c0adf6361d009f06f62df785be6a53c))

# [1.8.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.7.0...v1.8.0) (2026-01-19)


### Features

* pace streaming with buffered output and underrun handling ([b7a024e](https://github.com/lox-audioserver/lox-linein-bridge/commit/b7a024eeeddbe6278622e112294af5e1f24d42c2))

# [1.7.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.6.3...v1.7.0) (2026-01-19)


### Features

* improve streaming stability and report observed input rate ([8c116eb](https://github.com/lox-audioserver/lox-linein-bridge/commit/8c116eb8d37ccd1ea9e7493936c0b71f86207f9f))
* rediscover server after repeated status failures ([6d5e4aa](https://github.com/lox-audioserver/lox-linein-bridge/commit/6d5e4aa311c7144bb8d99417d2d394204cde9a3e))

## [1.6.3](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.6.2...v1.6.3) (2026-01-19)


### Bug Fixes

* log observed input sample rate regularly ([a11c0ee](https://github.com/lox-audioserver/lox-linein-bridge/commit/a11c0eeb91e9998d830733d6755baa73c4b834cc))

## [1.6.2](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.6.1...v1.6.2) (2026-01-19)


### Bug Fixes

* track actual input rate and log drift ([79f29a6](https://github.com/lox-audioserver/lox-linein-bridge/commit/79f29a68bc70cc1694ed2c9053c87ba04e1ce9d1))

## [1.6.1](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.6.0...v1.6.1) (2026-01-19)


### Bug Fixes

* log bridge stream throughput for rate debugging ([2e24eab](https://github.com/lox-audioserver/lox-linein-bridge/commit/2e24eab78ed44d3eb5ec6b3e5ecf147d38b0201b))

# [1.6.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.5.0...v1.6.0) (2026-01-19)


### Features

* make ingest sample rate server-driven and resample dynamically ([37704b5](https://github.com/lox-audioserver/lox-linein-bridge/commit/37704b5d380f138932f7b915725857665731a520))

# [1.5.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.4.0...v1.5.0) (2026-01-18)


### Features

* apply VAD updates live without restarting ingest ([30ee908](https://github.com/lox-audioserver/lox-linein-bridge/commit/30ee9089ddb83b3f7706a9627da0390af4d93d51))

# [1.4.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.3.0...v1.4.0) (2026-01-18)


### Features

* add mDNS discovery, bridge registration, server-driven config, and install flow updates ([a00a736](https://github.com/lox-audioserver/lox-linein-bridge/commit/a00a7369a1cd7e8f4a7da330c8317a9f8d9cc7e2))
* switch bridge to mDNS discovery with server registration and config-driven streaming ([d483437](https://github.com/lox-audioserver/lox-linein-bridge/commit/d4834370f430dfd02f582112bbc0d3f676006971))

# [1.3.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.2.0...v1.3.0) (2026-01-18)


### Bug Fixes

* force ALSA host for input device discovery ([1fa109b](https://github.com/lox-audioserver/lox-linein-bridge/commit/1fa109b479dfd04aec1137138e6f94ba132be7f7))


### Features

* switch to bridge-based discovery/registration with server-driven config and systemd install ([ff4e1fb](https://github.com/lox-audioserver/lox-linein-bridge/commit/ff4e1fb06ac76d714c9bfff8f644a77792c72a8f))

# [1.2.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.1.0...v1.2.0) (2026-01-18)


### Features

* improve runtime logging and VAD defaults ([148a794](https://github.com/lox-audioserver/lox-linein-bridge/commit/148a7948bd6b48bb8c58ed68fd11a0741a6be52e))

# [1.1.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.0.0...v1.1.0) (2026-01-18)


### Features

* add VAD gating, health output, ALSA log silencing, and release workflow fixes ([ceb984d](https://github.com/lox-audioserver/lox-linein-bridge/commit/ceb984de499df8d52d7abfbd852bea52b7013a46))

# [1.1.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.0.0...v1.1.0) (2026-01-18)


### Features

* add VAD gating, health output, ALSA log silencing, and release workflow fixes ([ceb984d](https://github.com/lox-audioserver/lox-linein-bridge/commit/ceb984de499df8d52d7abfbd852bea52b7013a46))

# [1.1.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.0.0...v1.1.0) (2026-01-18)


### Features

* add VAD gating, health output, ALSA log silencing, and release workflow fixes ([ceb984d](https://github.com/lox-audioserver/lox-linein-bridge/commit/ceb984de499df8d52d7abfbd852bea52b7013a46))

# [1.1.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.0.0...v1.1.0) (2026-01-18)


### Features

* add VAD gating, health output, ALSA log silencing, and release workflow fixes ([ceb984d](https://github.com/lox-audioserver/lox-linein-bridge/commit/ceb984de499df8d52d7abfbd852bea52b7013a46))

# [1.1.0](https://github.com/lox-audioserver/lox-linein-bridge/compare/v1.0.0...v1.1.0) (2026-01-18)


### Features

* add VAD gating, health output, ALSA log silencing, and release workflow fixes ([ceb984d](https://github.com/lox-audioserver/lox-linein-bridge/commit/ceb984de499df8d52d7abfbd852bea52b7013a46))

# 1.0.0 (2026-01-18)


### Features

* initial release setup ([6ec9b2a](https://github.com/lox-audioserver/lox-linein-bridge/commit/6ec9b2afde0d2171e30e5abefc114a1d0ffa2c19))
