# ADR 0003: Metadata Store

## Status
Accepted

## Context
Single-node mode needs transactional metadata and migrations.

## Decision
Use SQLite via Poco::Data for Milestone 2. Migrate to PostgreSQL in Milestone 4/5.

## Consequences
- Simple local setup.
- Single-node limitations; requires migration path for distributed mode.
