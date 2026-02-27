/**
 * test_aws_initializer.cpp - Unit tests for AWSInitializer singleton
 */

#include "levelii/AWSInitializer.h"
#include <iostream>
#include <chrono>
#include <cassert>

int main() {
    std::cout << "=== Testing AWSInitializer ===" << std::endl;

    std::cout << "\nTest 1: Singleton instance creation" << std::endl;
    auto& initializer1 = AWSInitializer::instance();
    auto& initializer2 = AWSInitializer::instance();
    assert(&initializer1 == &initializer2);
    std::cout << "✅ Singleton pattern works - same instance" << std::endl;

    std::cout << "\nTest 2: Initialize AWS SDK" << std::endl;
    assert(!initializer1.is_initialized());
    auto start = std::chrono::steady_clock::now();
    initializer1.initialize();
    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    assert(initializer1.is_initialized());
    std::cout << "✅ AWS SDK initialized in " << elapsed_ms << "ms" << std::endl;
    if (elapsed_ms < 100) {
        std::cout << "✅ Initialization latency < 100ms (excellent!)" << std::endl;
    } else if (elapsed_ms < 500) {
        std::cout << "⚠️  Initialization latency " << elapsed_ms << "ms (acceptable)" << std::endl;
    } else {
        std::cout << "⚠️  Initialization latency " << elapsed_ms << "ms (slower than expected)" << std::endl;
    }

    std::cout << "\nTest 3: Get S3 client" << std::endl;
    auto s3_client = initializer1.get_s3_client();
    assert(s3_client != nullptr);
    std::cout << "✅ S3Client retrieved successfully" << std::endl;

    std::cout << "\nTest 4: Idempotent initialization" << std::endl;
    auto start2 = std::chrono::steady_clock::now();
    initializer1.initialize();
    auto end2 = std::chrono::steady_clock::now();
    auto elapsed_ms2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2).count();
    std::cout << "✅ Second initialize() call (should be quick): " << elapsed_ms2 << "ms" << std::endl;

    std::cout << "\nTest 5: Shutdown" << std::endl;
    initializer1.shutdown();
    assert(!initializer1.is_initialized());
    std::cout << "✅ AWS SDK shutdown complete" << std::endl;

    std::cout << "\n=== All Tests Passed ===" << std::endl;
    return 0;
}
