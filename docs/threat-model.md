# Threat Model

## Assets
- User data (object contents and metadata)
- Authentication tokens (Milestone 3)
- Service availability

## Threats and Mitigations (Current)

- **Path traversal**: object paths are validated and normalized.
- **Large uploads (DoS)**: size limits enforced per request.
- **Replay / auth bypass**: not applicable yet (auth pending).

## Planned Mitigations

- OAuth2/OIDC JWT validation with JWKS caching.
- Rate limiting per IP / token.
- TLS with modern cipher suites and rotation guide.
- Audit logging and correlation IDs.
