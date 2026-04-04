# Product Requirements Document (PRD) - Parser Fixes

## Problem Description
The system is experiencing a `munmap_chunk(): invalid pointer` error in the NEXRAD Level II parser. This error typically indicates heap corruption, such as a double free, freeing an invalid pointer, or memory corruption that overwrites heap metadata.

## Goals
- Reproduce the `munmap_chunk()` error.
- Identify the root cause of the heap corruption.
- Implement a fix to ensure stable parsing.
- Verify the fix using existing and new tests.

## Research Findings
- The codebase uses modern C++ (C++17) with smart pointers and `std::vector` for most memory management.
- There are no manual `malloc`/`free` or `new`/`delete` calls in the core `src/` directory.
- `BufferPool` in `BackgroundFrameFetcher.cpp` manages a pool of `std::vector<uint8_t>` objects, using raw pointers for acquisition/release.
- `DecompressionUtils.cpp` uses `bzlib` with manual buffer management via `bz_stream`.
- `RadarParser.cpp` uses `reinterpret_cast` to read packed structs from byte arrays.
- `shrink_to_fit()` and `std::vector::swap` are used to reclaim memory, which can trigger reallocations and `munmap`.

### Suspected Areas
1. **BufferPool Race Conditions**: Although protected by a mutex, any use of a buffer after it has been released back to the pool (use-after-free) would cause corruption.
2. **bzlib Buffer Management**: In `DecompressionUtils.cpp`, the `stream.next_out` and `stream.avail_out` are updated manually. If these are calculated incorrectly, `bzlib` might write past the end of the `vector`'s allocated memory.
3. **Invalid Struct Mapping**: If `reinterpret_cast` is used on a pointer that doesn't actually contain enough data (despite bounds checks), it could lead to reading/writing invalid memory.
4. **Vector Corruption**: `shrink_to_fit()` or `std::vector::swap` might be interacting poorly with the `BufferPool` if the pointers held in the pool are not properly synchronized with the vectors' lifecycles.

## Requirements

### 1. Robust Reproduction
- Create a test case that triggers the `munmap_chunk()` error reliably.
- Focus on concurrency and large/corrupted NEXRAD files.

### 2. Memory Safety in Decompression
- Ensure `bz_stream` pointers are always within the bounds of the `std::vector`.
- Validate `total_out_lo32` and other `bzlib` outputs before using them to resize vectors.
- Consider using a more robust wrapper for `bzlib` if necessary.

### 3. BufferPool Integrity
- Verify that `ScopedBuffer` and `BufferPool` are thread-safe and prevent double-release.
- Ensure that once a buffer is released, no pointers to its internal data are held.

### 4. Struct Parsing Safety
- Review all `reinterpret_cast` calls and ensure they are preceded by rigorous bounds checks.
- Use `nexrad::safe_read_struct` everywhere possible.

## Verification Plan
- **Unit Tests**: Run all existing unit tests in `build/test/code_tests/unit/`.
- **Integration Tests**: Run `benchmark_memory_concurrency` and `deadlock_simulation`.
- **Fuzz Tests**: Run `test_fuzz_corrupt_data` with extended iterations.
- **Valgrind/ASan**: Run the parser under AddressSanitizer (ASan) or Valgrind to detect the exact point of corruption.

## Open Questions
- Does the error happen only under high concurrency?
- Is it triggered by a specific type of NEXRAD file (e.g., specific compression or version)?
- Are there any 3rd-party libraries (like `aws-cpp-sdk`) that might be contributing to the heap corruption?
