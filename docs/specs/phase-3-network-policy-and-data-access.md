# Phase 3: Network Policy and Data Access

## Goal

- enable controlled remote access for agent workflows
- prevent network capability from becoming an SSRF or credential leak surface

## Required Scope

- network disabled by default
- URL allowlisting by origin and optional path prefix
- allowed HTTP method policy
- redirect revalidation
- request timeout
- response size limit
- protocol restrictions
- optional private range blocking
- optional DNS rebinding protection
- host-side credential/header injection model

## Acceptance Criteria

- network commands are unavailable unless network policy is explicitly configured
- disallowed origins and paths are blocked with sanitized errors
- disallowed HTTP methods are blocked
- redirects to non-allowed targets are blocked
- oversized responses are rejected safely
- private and loopback destinations are blocked when private-range protection is enabled
- credential injection occurs outside the sandbox and is not exposed as plain script data unless explicitly intended
- policy behavior is covered by automated tests for allowlists, redirects, and SSRF-related cases
