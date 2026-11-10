cmake_minimum_required(VERSION 3.22)
project(FraudRingEngine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)
add_library(fraud_core src/engines/brute_force.cpp)
target_include_directories(fraud_core PUBLIC src)
add_executable(phase1_bench bench/phase1.cpp)
target_link_libraries(phase1_bench PRIVATE fraud_core)

if(BUILD_TESTING)
  find_package(GTest REQUIRED)
  add_executable(core_tests tests/core_test.cpp)
  target_link_libraries(core_tests PRIVATE fraud_core GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(core_tests DISCOVERY_MODE PRE_TEST)
  add_executable(toolchain_smoke tests/toolchain_smoke.cpp)
  add_test(NAME toolchain_smoke
    COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}"
            --config "$<CONFIG>" --target toolchain_smoke)
endif()
