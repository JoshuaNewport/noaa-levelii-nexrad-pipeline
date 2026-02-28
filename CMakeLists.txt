cmake_minimum_required(VERSION 3.15)
project(LevelIIProcessor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Options
option(ENABLE_TESTING "Build test executables" OFF)

# Find Required Packages
find_package(CURL REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS s3 core)

# ============================================================================
# Level II Decompression Utilities Library
# ============================================================================

add_library(levelii_DecompressionUtils STATIC
    src/DecompressionUtils.cpp
)

target_include_directories(levelii_DecompressionUtils PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${BZIP2_INCLUDE_DIR}
)

target_link_libraries(levelii_DecompressionUtils PUBLIC
    ${BZIP2_LIBRARIES}
)

# ============================================================================
# Level II Frame Storage Manager Library
# ============================================================================

add_library(levelii_FrameStorageManager STATIC
    src/FrameStorageManager.cpp
    src/ZlibUtils.cpp
)

target_include_directories(levelii_FrameStorageManager PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${ZLIB_INCLUDE_DIRS}
)

target_link_libraries(levelii_FrameStorageManager PRIVATE stdc++fs ZLIB::ZLIB)

# ============================================================================
# Level II Radar Parser Library
# ============================================================================

add_library(levelii_RadarParser STATIC
    src/RadarParser.cpp
    src/RadarFrame.cpp
    src/VolumetricGenerator.cpp
)

target_include_directories(levelii_RadarParser PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(levelii_RadarParser PUBLIC
    levelii_DecompressionUtils
)

# ============================================================================
# AWS Initializer Library
# ============================================================================

add_library(levelii_AWSInitializer STATIC
    src/AWSInitializer.cpp
)

target_include_directories(levelii_AWSInitializer PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${AWSSDK_INCLUDE_DIRS}
)

target_link_libraries(levelii_AWSInitializer PUBLIC
    aws-cpp-sdk-s3
    aws-cpp-sdk-sns
    aws-cpp-sdk-sqs
    aws-cpp-sdk-core
)

# ============================================================================
# Thread Pool Library
# ============================================================================

add_library(levelii_ThreadPool STATIC
    src/ThreadPool.cpp
)

target_include_directories(levelii_ThreadPool PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(levelii_ThreadPool PUBLIC
    pthread
)

# ============================================================================
# Level II Background Frame Fetcher Library
# ============================================================================

add_library(levelii_BackgroundFrameFetcher STATIC
    src/BackgroundFrameFetcher.cpp
    src/TerminalUI.cpp
)

target_include_directories(levelii_BackgroundFrameFetcher PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CURL_INCLUDE_DIRS}
    ${AWSSDK_INCLUDE_DIRS}
)

target_link_libraries(levelii_BackgroundFrameFetcher PUBLIC
    levelii_FrameStorageManager
    levelii_DecompressionUtils
    levelii_RadarParser
    levelii_AWSInitializer
    levelii_ThreadPool
    ${CURL_LIBRARIES}
    pthread
)

# ============================================================================
# Admin and Web Server Library
# ============================================================================

add_library(levelii_Admin STATIC
    src/admin/AdminAPI.cpp
    src/admin/AdminServer.cpp
    src/admin/WebServer.cpp
)

target_include_directories(levelii_Admin PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(levelii_Admin PUBLIC
    levelii_BackgroundFrameFetcher
)

# ============================================================================
# Main Processing Executables
# ============================================================================

add_executable(process_to_volumetric src/process_to_volumetric.cpp)
target_include_directories(process_to_volumetric PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(process_to_volumetric PRIVATE levelii_FrameStorageManager levelii_RadarParser)

add_executable(nexrad_pipeline src/nexrad_pipeline.cpp)
target_include_directories(nexrad_pipeline PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(nexrad_pipeline PRIVATE 
    levelii_Admin
    levelii_BackgroundFrameFetcher
    levelii_FrameStorageManager
    levelii_RadarParser
    levelii_DecompressionUtils
    levelii_AWSInitializer
    levelii_ThreadPool
    ${CURL_LIBRARIES}
    ${BZIP2_LIBRARIES}
    ZLIB::ZLIB
    aws-cpp-sdk-s3
    aws-cpp-sdk-core
    pthread
)

# ============================================================================
# Subdirectories
# ============================================================================

if(ENABLE_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()
