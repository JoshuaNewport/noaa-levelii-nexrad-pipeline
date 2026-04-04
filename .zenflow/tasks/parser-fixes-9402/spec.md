# Technical Specification - Parser Fixes

## Problem Description
The system is experiencing a `munmap_chunk(): invalid pointer` error in the NEXRAD Level II parser. This error indicates heap corruption, typically occurring during memory deallocation.

## Technical Context
- **Language**: C++17
- **Dependencies**: `bzlib`, `zlib`, `aws-cpp-sdk-s3`
- **Key Components**: `BufferPool`, `DecompressionUtils`, `RadarParser`, `MessageSegmenter`

## Root Cause Analysis
Based on investigation, the following areas are suspected:
1. **BufferPool Race Conditions**: While `BufferPool` uses a mutex, the reconfiguration process in `BackgroundFrameFetcher::reinitialize_pools` might lead to issues if buffers from an old pool are released after the pool has been shut down, although `shared_ptr` should handle this.
2. **bzlib Buffer Management**: In `DecompressionUtils.cpp`, `std::vector::resize` is called which can reallocate. If `bzlib` holds any internal state relative to the old pointer, it could cause corruption. However, `bz_stream` is supposed to be self-contained.
3. **Incomplete Struct Definitions**: `Message31Header` in `NEXRAD_Types.h` is incomplete, which leads to incorrect offsets for `block_pointers`. While `safe_pointer_dereference` should prevent crashes, it leads to incorrect parsing.
4. **Duplicate Data Insertion**: `BackgroundFrameFetcher.cpp` line 491 appears to duplicate the last read block from S3 if the loop finishes due to EOF.

## Implementation Approach

### 1. Robust Decompression
- Update `DecompressionUtils.cpp` to ensure `bz_stream` is always properly initialized and ended.
- Use a safer approach for growing vectors during decompression to avoid frequent reallocations.
- Add explicit clearing of output vectors before decompression.
- Validate `total_out_lo32` and `total_out_hi32` to handle potential 32-bit wrap-around (though unlikely for single frames).

### 2. Strengthen BufferPool
- Ensure `BufferPool` release logic is perfectly thread-safe even during shutdown.
- Add more logging to `BufferPool` to track acquisition and release.
- Consider using `std::vector::shrink_to_fit()` instead of `swap()` if it provides better stability on the target platform.

### 3. Fix Struct Definitions and Parsing
- Complete the `Message31Header` struct definition according to NEXRAD ICD 2620010J.
- Review all `reinterpret_cast` calls and ensure they are aligned or use `std::memcpy` (already mostly done via `read_be`).
- Improve `MessageSegmenter` to handle corrupted or out-of-order segments more gracefully.

### 4. S3 Fetching Fixes
- Fix the potential double-insertion of data in `BackgroundFrameFetcher::process_discovery_batch`.

## Source Code Structure Changes
- **include/levelii/NEXRAD_Types.h**: Update `Message31Header` and related structs.
- **src/DecompressionUtils.cpp**: Refactor decompression loops for better safety.
- **src/BackgroundFrameFetcher.cpp**: Fix S3 read loop and improve `BufferPool` logging.

## Verification Plan
- **AddressSanitizer (ASan)**: Run benchmark and integration tests with ASan enabled.
- **Reproduction Script**: Run `test/code_tests/integration/repro_munmap.cpp` with high concurrency and long duration.
- **Fuzz Testing**: Run `test_fuzz_corrupt_data` with extended iterations and corrupted Archive II headers.
- **Memory Profiling**: Monitor RSS growth during benchmark to ensure no leaks.

## Delivery Phases
1. **Phase 1: Integrity Fixes**: Fix struct definitions and S3 fetching duplication.
2. **Phase 2: Decompression Safety**: Refactor `DecompressionUtils.cpp` for maximum robustness.
3. **Phase 3: BufferPool Hardening**: Improve `BufferPool` stability and logging.
4. **Phase 4: Validation**: Extensive testing under load and with ASan.
