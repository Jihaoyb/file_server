# ADR 0001: HTTP Stack

## Status
Accepted

## Context
We need async HTTP with TLS support, streaming bodies, and fine-grained control.

## Decision
Use Boost.Asio + Boost.Beast for networking/HTTP and OpenSSL for TLS.

## Consequences
- Full control over connection lifecycle and performance.
- More code to maintain than a higher-level framework.
