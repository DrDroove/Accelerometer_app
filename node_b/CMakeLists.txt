cmake_minimum_required(VERSION 3.16)
project(AccelNodeB CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/../proto/accelerometer.proto")
set(GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GEN_DIR})

find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)

add_custom_command(
    OUTPUT 
        "${GEN_DIR}/accelerometer.pb.cc"
        "${GEN_DIR}/accelerometer.pb.h"
        "${GEN_DIR}/accelerometer.grpc.pb.cc"
        "${GEN_DIR}/accelerometer.grpc.pb.h"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out=${GEN_DIR}
         --cpp_out=${GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         -I "${CMAKE_CURRENT_SOURCE_DIR}/../proto"
         "${PROTO_FILE}"
    DEPENDS ${PROTO_FILE}
)

add_library(accel_proto_client
    "${GEN_DIR}/accelerometer.pb.cc"
    "${GEN_DIR}/accelerometer.grpc.pb.cc"
)
target_include_directories(accel_proto_client PUBLIC ${GEN_DIR})
target_link_libraries(accel_proto_client PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_executable(node_b main.cpp)
target_link_libraries(node_b PRIVATE accel_proto_client)