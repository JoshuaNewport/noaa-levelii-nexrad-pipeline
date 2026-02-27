/**
 * AWSInitializer.h - Global AWS SDK initialization and S3 client management
 *
 * Singleton pattern for eager AWS SDK initialization at application startup.
 * Provides a reusable S3Client instance shared across the application.
 *
 * This reduces initialization latency from 1-2 seconds (per-instance) to
 * <50ms via global initialization + connection warm-up on app startup.
 */

#pragma once

#include <memory>
#include <mutex>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

class AWSInitializer {
public:
    static AWSInitializer& instance();

    bool is_initialized() const { return initialized_; }

    std::shared_ptr<Aws::S3::S3Client> get_s3_client() const {
        return s3_client_;
    }

    void initialize();
    
    void initialize_async();

    void shutdown();

    ~AWSInitializer();

private:
    AWSInitializer();

    static std::unique_ptr<AWSInitializer> instance_;
    static std::mutex instance_mutex_;

    bool initialized_{false};
    Aws::SDKOptions aws_options_;
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    std::mutex state_mutex_;
};
