/**
 * AWSInitializer.cpp - Implementation
 */

#include "levelii/AWSInitializer.h"
#include <iostream>
#include <chrono>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>

std::unique_ptr<AWSInitializer> AWSInitializer::instance_ = nullptr;
std::mutex AWSInitializer::instance_mutex_;

AWSInitializer::AWSInitializer() = default;

AWSInitializer::~AWSInitializer() {
    shutdown();
}

AWSInitializer& AWSInitializer::instance() {
    std::lock_guard<std::mutex> lock(instance_mutex_);
    if (!instance_) {
        instance_ = std::unique_ptr<AWSInitializer>(new AWSInitializer());
    }
    return *instance_;
}

void AWSInitializer::initialize() {
    std::lock_guard<std::mutex> lock(state_mutex_);

    if (initialized_) {
        return;
    }

    auto start_time = std::chrono::steady_clock::now();

    Aws::InitAPI(aws_options_);

    Aws::Client::ClientConfiguration aws_config;
    aws_config.region = "us-east-1";
    aws_config.connectTimeoutMs = 5000;
    aws_config.requestTimeoutMs = 5000;
    aws_config.maxConnections = 50;

    // NEXRAD bucket is public, anonymous access is faster and avoids credential timeouts
    auto anon_creds_provider = std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>();
    
    s3_client_ = std::make_shared<Aws::S3::S3Client>(
        anon_creds_provider,
        aws_config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        true
    );

    initialized_ = true;

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "⚡ AWS SDK (S3-only) initialized in " << elapsed_ms << "ms" << std::endl;
}

void AWSInitializer::initialize_async() {
    // For now, just call initialize() directly as it's much faster now
    initialize();
}

void AWSInitializer::shutdown() {
    std::lock_guard<std::mutex> lock(state_mutex_);

    if (!initialized_) {
        return;
    }

    s3_client_.reset();
    Aws::ShutdownAPI(aws_options_);
    initialized_ = false;

    std::cout << "✅ AWS SDK shutdown complete" << std::endl;
}
