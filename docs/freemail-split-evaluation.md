# Freemail Split Evaluation

## Version Scope

This note evaluates the current `freemail` registration flow after the retry-state split work landed, while keeping the existing queue-yield and deferred-execution direction unchanged.

## Evaluation

The current `freemail` version is meaningfully stronger than the earlier monolithic task flow. It has moved from "can run" to "can run with control". The improvement is not based on extending wait times. It comes from turning common `freemail` registration failures into structured, observable, short-deferral decisions that still preserve overall batch throughput.

## Strengths

- `freemail` failures such as OTP wait timeout now enter a short deferred retry path instead of being treated as immediate hard failures.
- Task visibility is better because the registration task now exposes `phase`, `reason_code`, `retry_count`, `defer_bucket`, and `next_retry_at`.
- The legacy batch yield branch has been unified into the same retry-state model, but it still keeps the short batch retry window, so the overall queue pace is not slowed down.
- The implementation scope remains narrow and local to the registration feature. It does not introduce broad cross-project refactors.

## Current Boundary

- This is not yet a true phase-resume implementation. It is a closed-loop retry-state upgrade, not a full resume-from-phase executor.
- Some branches are still compatibility-oriented rather than fully isolated `freemail`-specific execution paths.
- Future work should continue splitting email-context rotation, domain switching, and phase resume in small steps instead of adding more branching to the current route layer.

## Conclusion

This `freemail` version is suitable as a stable verification baseline. The direction is correct, the gains are real, and the risk has been kept under control.
