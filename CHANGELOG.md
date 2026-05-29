# Changelog

All notable MoatX version changes are recorded here.

## 1.0.0 - 2026-05-30

### Added
- Added intraday anomaly radar for minute-level A-share moves.
- Added CLI entry: `python -m modules.cli tool intraday`.
- Added single-stock intraday replay and stock-pool radar scan.
- Added sector resonance scoring for synchronized moves across the same theme.
- Added unified runtime tag lookup through `SectorTagProvider` for radar use.
- Added short-term strategy backtest support with fixed stock pools and diagnostic attribution.
- Added short-term watchlist, paper account, target price, stop-loss, and next-day review workflow.

### Changed
- Promoted project version from `0.1.0` to `1.0.0`.
- Promoted project classifier from Alpha to Production/Stable.
- Unified `sector_graph.toml` as the main sector/theme graph and `stock_topic_exposure.toml` as the stock-topic exposure overlay.
- Changed intraday sector resonance to use the unified `SectorTagProvider` instead of reading TOML files directly.
- Improved near-synonym theme matching for themes such as electricity, precious metals, chips, and semiconductors.
- Improved short-term scoring with historical reference, risk gates, news factors, theme exposure, and market confirmation.

### Fixed
- Fixed Python 3.14 / akshare fallback behavior so optional datasource failures no longer block the main analysis path.
- Fixed announcement filtering to avoid mixing unrelated company announcements into stock reports.
- Fixed market-session judgment around post-close time windows.
- Fixed sector graph cache isolation so different graph paths do not share stale cached content.

### Verified
- Verified package version resolves to `1.0.0`.
- Verified intraday radar on the 2026-05-29 electricity sample pool.
- Verified sector resonance can boost non-electricity themes such as chip-related stocks.
- Verified unified tag lookup for representative stocks including Sichuan Gold, Tongfu Microelectronics, Goertek, and Huaneng Power.
