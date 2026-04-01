# Phase 5: Compatibility Expansion

## Goal

- increase usefulness for real agent workflows while preserving safety guarantees
- expand shell coverage intentionally rather than opportunistically

## Required Scope

- broader support for common shell composition features
- additional high-value builtins and text-processing workflows
- compatibility matrix for supported shell features
- explicit backlog of known limitations and unsupported features

## Acceptance Criteria

- the project publishes a maintained compatibility statement
- newly added shell features are covered by behavioral regression tests
- unsupported or partial features are explicitly documented
- no new shell feature is marked complete without corresponding security and limit checks
- feature expansion does not silently widen host access or privilege
