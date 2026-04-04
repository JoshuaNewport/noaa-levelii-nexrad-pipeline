# Full SDD workflow

## Configuration
- **Artifacts Path**: {@artifacts_path} → `.zenflow/tasks/{task_id}`

---

## Agent Instructions

---

## Workflow Steps

### [x] Step: Requirements
<!-- chat-id: 5731b859-5b3c-46bb-a1dd-77d4239bc447 -->

Create a Product Requirements Document (PRD) based on the feature description.

1. Review existing codebase to understand current architecture and patterns
2. Analyze the feature definition and identify unclear aspects
3. Ask the user for clarifications on aspects that significantly impact scope or user experience
4. Make reasonable decisions for minor details based on context and conventions
5. If user can't clarify, make a decision, state the assumption, and continue

Save the PRD to `{@artifacts_path}/requirements.md`.

### [x] Step: Technical Specification
<!-- chat-id: f597909f-ddf4-4ba6-95d3-1ea596e503a2 -->

Create a technical specification based on the PRD in `{@artifacts_path}/requirements.md`.

1. Review existing codebase architecture and identify reusable components
2. Define the implementation approach

Save to `{@artifacts_path}/spec.md` with:
- Technical context (language, dependencies)
- Implementation approach referencing existing code patterns
- Source code structure changes
- Data model / API / interface changes
- Delivery phases (incremental, testable milestones)
- Verification approach using project lint/test commands

### [x] Step: Planning
<!-- chat-id: 3097bccb-f1a4-472c-bb99-4f012ad0a0e2 -->

Create a detailed implementation plan based on `{@artifacts_path}/spec.md`.

1. Break down the work into concrete tasks
2. Each task should reference relevant contracts and include verification steps
3. Replace the Implementation step below with the planned tasks

Rule of thumb for step size: each step should represent a coherent unit of work (e.g., implement a component, add an API endpoint). Avoid steps that are too granular (single function) or too broad (entire feature).

Important: unit tests must be part of each implementation task, not separate tasks. Each task should implement the code and its tests together, if relevant.

If the feature is trivial and doesn't warrant full specification, update this workflow to remove unnecessary steps and explain the reasoning to the user.

Save to `{@artifacts_path}/plan.md`.

### [x] Step: Phase 1: Integrity Fixes
<!-- chat-id: a3e43200-a1fc-44d9-b7d4-80310d4b3d95 -->
- Update `include/levelii/NEXRAD_Types.h` to complete `Message31Header` struct definition.
- Fix S3 read loop in `src/BackgroundFrameFetcher.cpp` to prevent double-insertion and improve robustness.
- **Verification**: Run `test_fuzz_corrupt_data` and verify no struct-related crashes.

### [ ] Step: Phase 2: Decompression Safety
- Refactor `src/DecompressionUtils.cpp` to use safer vector growth logic and handle potential 32-bit wrap-around.
- Add explicit `decompressed.clear()` and error checks for `BZ2_bzDecompress` return values.
- **Verification**: Run `unit_tests` in `test/code_tests/unit/`.

### [ ] Step: Phase 3: BufferPool Hardening
- Add logging to `BufferPool` acquisition/release and shutdown.
- Ensure `BufferPool::release` is robust against double-release and shutdown races.
- **Verification**: Run `deadlock_simulation` and `repro_munmap` scripts.

### [ ] Step: Phase 4: Validation
- Build with AddressSanitizer (ASan) and run `benchmark_memory_concurrency`.
- Verify no memory errors are reported during long-duration runs of `repro_munmap`.
- **Verification**: All integration tests pass under ASan.
