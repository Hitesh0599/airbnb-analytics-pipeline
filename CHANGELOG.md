# Changelog

All notable changes to this project are documented here.

## [v2.0] - 2026-10-21

### Changed: deletion handling now uses `deletion_flag`

In v1, deletions were identified by a column marker in the source data. In v2, each source record carries an explicit `deletion_flag`, and the pipeline treats it as the single source of truth for whether a record has been deleted.

- **Dedup layer:** keeps the latest row per business key and uses `deletion_flag` to identify records that have been deleted at the source. This is the only layer where deletions are handled.
- **Snapshots (SCD Type 2):** records flagged as deleted in the dedup layer have their validity stopped (`dbt_valid_to` is set) and no new current row is created. Their past versions stay in the history table.
- **Downstream models:** intermediate and mart models need no deletion filter of their own. They rely on the snapshot validity, so a deleted record simply has no current version.
- **Tests:** added data tests and unit tests covering the updated deletion logic.

### Added: cascading deletes from hosts to listings

When a host is deleted, their listings are deleted with them.

- A host with `deletion_flag` set marks all listings with that `host_id` as deleted, effective from the same point in time as the host deletion.
- This prevents orphaned listings, meaning active listings whose host no longer exists.
- Listing history is preserved through the snapshots, so point-in-time joins still return the correct host and listing state for bookings made before the deletion.
- Added tests to check that no active listing points to a deleted host.

### Updated: full dataset
Note: This is a generated data

| Metric | v1 | v2 |
|---|---|---|
| Hosts | 200 | 2,198 |
| Listings | 500 | 3,994 |
| Bookings | n/a | 82,471 |
| Revenue | $3.42M | $53.40M |
| Period | 25 Dec 2024 to 25 Dec 2025 | 25 Dec 2024 to 25 Dec 2025 |

### Updated: dashboard and documentation

- Refreshed the Power BI dashboard with the new data (revenue, cancellation rate, full-moon impact, host type, property type, continent and top listings).
- Rewrote the README observations to match the new data.
- Replaced the project banner with the updated statistics.

### Housekeeping

- Removed `.user.yml` from version control and added it to `.gitignore`.

## [v1.0]

- Initial release: S3 to Snowflake to dbt to Power BI pipeline with raw, staging, dedup, snapshot, intermediate and mart layers, SCD Type 2 history, point-in-time joins, data and unit tests, and an audit log.