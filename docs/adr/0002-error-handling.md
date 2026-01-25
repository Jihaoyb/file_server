# ADR 0002: Error Handling

## Status
Accepted

## Context
We need explicit errors without exceptions crossing module boundaries.

## Decision
Use a lightweight `Result<T>` type for internal APIs. Map errors to HTTP JSON envelopes.

## Consequences
- Callers must check results.
- Clear error propagation and consistent HTTP responses.
