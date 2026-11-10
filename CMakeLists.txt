cmake_minimum_required(VERSION 3.22)
project(FraudRingEngine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)
if(BUILD_TESTING)
  add_executable(toolchain_smoke tests/toolchain_smoke.cpp)
  add_test(NAME toolchain_smoke
    COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}"
            --config "$<CONFIG>" --target toolchain_smoke)
endif()
