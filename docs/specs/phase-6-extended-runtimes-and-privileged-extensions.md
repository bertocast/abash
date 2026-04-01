# Phase 6: Extended Runtimes and Privileged Extensions

## Goal

- add higher-capability execution surfaces only after the base shell environment is stable
- keep privileged extension points explicitly separated from untrusted script execution

## Required Scope

- opt-in additional runtimes, if any
- explicit trust model for custom commands, plugins, or transforms
- privileged extension documentation
- stronger isolation requirements for any feature that executes real host interpreters or binaries

## Acceptance Criteria

- optional runtimes are disabled by default
- each optional runtime is documented as additional security surface
- each optional runtime has dedicated timeout and resource controls
- custom commands and extension hooks are documented as privileged host code
- if any feature executes real host shell or binaries, the product requires and documents a real OS-level isolation boundary
- no optional capability is exposed through the default safe profile
