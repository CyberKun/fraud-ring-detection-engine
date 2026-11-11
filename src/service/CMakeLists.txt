find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
set(protocol_dir "${PROJECT_SOURCE_DIR}/proto")
set(generated "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${generated}")
add_custom_command(
  OUTPUT "${generated}/fraud_engine.pb.cc" "${generated}/fraud_engine.pb.h"
         "${generated}/fraud_engine.grpc.pb.cc" "${generated}/fraud_engine.grpc.pb.h"
         "${generated}/snapshot.pb.cc" "${generated}/snapshot.pb.h"
  COMMAND protobuf::protoc -I "${protocol_dir}" --cpp_out=${generated} fraud_engine.proto snapshot.proto
  COMMAND protobuf::protoc -I "${protocol_dir}" --grpc_out=${generated}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin> fraud_engine.proto
  DEPENDS "${protocol_dir}/fraud_engine.proto" "${protocol_dir}/snapshot.proto"
  VERBATIM)
add_library(fraud_proto "${generated}/fraud_engine.pb.cc" "${generated}/fraud_engine.grpc.pb.cc"
                        "${generated}/snapshot.pb.cc")
target_include_directories(fraud_proto PUBLIC "${generated}")
target_link_libraries(fraud_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
add_library(fraud_store ../storage/wal.cpp ../storage/snapshot.cpp)
target_link_libraries(fraud_store PUBLIC fraud_core fraud_proto ZLIB::ZLIB OpenSSL::Crypto)
add_library(fraud_node node.cpp server.cpp)
target_link_libraries(fraud_node PUBLIC fraud_store)
add_executable(fraud_server main.cpp)
target_link_libraries(fraud_server PRIVATE fraud_node)
set_target_properties(fraud_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
if(BUILD_TESTING)
  find_package(GTest REQUIRED)
  add_executable(storage_tests ../../tests/storage_test.cpp)
  target_link_libraries(storage_tests PRIVATE fraud_node GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(storage_tests DISCOVERY_MODE PRE_TEST)
  add_executable(phase5_determinism ../../bench/phase5_determinism.cpp)
  target_link_libraries(phase5_determinism PRIVATE fraud_node)
  find_package(CURL REQUIRED)
  add_executable(phase5_smoke ../../bench/phase5_smoke.cpp)
  target_link_libraries(phase5_smoke PRIVATE fraud_node CURL::libcurl)
  add_dependencies(phase5_smoke fraud_server)
  set_target_properties(storage_tests phase5_determinism phase5_smoke PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  add_test(NAME SingleNode.GrpcAndMetrics COMMAND phase5_smoke)
endif()
