# Tests CMakeLists.txt

# Unit tests
add_executable(test_parser_metadata unit/test_parser_metadata.cpp)
target_include_directories(test_parser_metadata PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_parser_metadata PRIVATE levelii_RadarParser)
add_test(NAME unit_parser_metadata COMMAND test_parser_metadata ${CMAKE_CURRENT_SOURCE_DIR}/../test_files/KTLX20260209_162244_V06)

add_executable(test_multi_product unit/test_multi_product.cpp)
target_include_directories(test_multi_product PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_multi_product PRIVATE levelii_RadarParser)
add_test(NAME unit_multi_product COMMAND test_multi_product ${CMAKE_CURRENT_SOURCE_DIR}/../test_files/KTLX20260209_162244_V06)

add_executable(test_threadpool unit/test_threadpool.cpp)
target_include_directories(test_threadpool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_threadpool PRIVATE levelii_ThreadPool)
add_test(NAME unit_threadpool COMMAND test_threadpool)

add_executable(test_aws_initializer unit/test_aws_initializer.cpp)
target_include_directories(test_aws_initializer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_aws_initializer PRIVATE levelii_AWSInitializer)
add_test(NAME unit_aws_initializer COMMAND test_aws_initializer)

add_executable(test_decompression unit/test_decompression.cpp)
target_include_directories(test_decompression PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_decompression PRIVATE levelii_RadarParser)
add_test(NAME unit_decompression COMMAND test_decompression)

add_executable(test_quantization unit/test_quantization.cpp)
target_include_directories(test_quantization PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_quantization PRIVATE levelii_RadarParser)
add_test(NAME unit_quantization COMMAND test_quantization)

add_executable(test_gate_indexing unit/test_gate_indexing.cpp)
target_include_directories(test_gate_indexing PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_gate_indexing PRIVATE levelii_RadarParser)
add_test(NAME unit_gate_indexing COMMAND test_gate_indexing)

add_executable(test_frame_storage unit/test_frame_storage.cpp)
target_include_directories(test_frame_storage PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_frame_storage PRIVATE levelii_FrameStorageManager)
add_test(NAME unit_frame_storage COMMAND test_frame_storage)

add_executable(test_config_manager unit/test_config_manager.cpp)
target_include_directories(test_config_manager PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_config_manager PRIVATE levelii_BackgroundFrameFetcher levelii_FrameStorageManager)
add_test(NAME unit_config_manager COMMAND test_config_manager)

# Integration tests
add_executable(test_real_data integration/test_real_data.cpp)
target_include_directories(test_real_data PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_real_data PRIVATE levelii_RadarParser)
add_test(NAME integration_real_data COMMAND test_real_data)

add_executable(test_volumetric_generation integration/test_volumetric_generation.cpp)
target_include_directories(test_volumetric_generation PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_volumetric_generation PRIVATE levelii_FrameStorageManager levelii_RadarParser)
add_test(NAME integration_volumetric_generation COMMAND test_volumetric_generation)

add_executable(check_sequence integration/check_sequence.cpp)
target_include_directories(check_sequence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(check_sequence PRIVATE levelii_RadarParser)
add_test(NAME integration_check_sequence COMMAND check_sequence ${CMAKE_CURRENT_SOURCE_DIR}/../test_files/KTLX20260209_162244_V06)

add_executable(benchmark_memory_concurrency integration/benchmark_memory_concurrency.cpp)
target_include_directories(benchmark_memory_concurrency PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(benchmark_memory_concurrency PRIVATE levelii_BackgroundFrameFetcher levelii_RadarParser levelii_FrameStorageManager)
add_test(NAME integration_benchmark_memory_concurrency COMMAND benchmark_memory_concurrency ${CMAKE_CURRENT_SOURCE_DIR}/../test_files/KTLX20260209_162244_V06)

# Fuzz tests
add_executable(test_fuzz_corrupt_data fuzz/test_fuzz_corrupt_data.cpp)
target_include_directories(test_fuzz_corrupt_data PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(test_fuzz_corrupt_data PRIVATE levelii_RadarParser)
add_test(NAME fuzz_corrupt_data COMMAND test_fuzz_corrupt_data ${CMAKE_CURRENT_SOURCE_DIR}/../test_files/KTLX20260209_162244_V06)
