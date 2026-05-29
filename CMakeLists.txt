cmake_minimum_required(VERSION 3.16)
project(AccelerometerDistributedSystem CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_CODEGEN ON CACHE BOOL "" FORCE)
set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    gRPC
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG        v1.62.0
    FIND_PACKAGE_ARGS CONFIG REQUIRED
)

FetchContent_Declare(
    Protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG        v26.0
    FIND_PACKAGE_ARGS REQUIRED
)

FetchContent_MakeAvailable(gRPC Protobuf)

set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_FILE "${PROTO_DIR}/accelerometer.proto")
set(GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GEN_DIR})

protobuf_generate(
    TARGETS shared_proto
    LANGUAGE cpp
    OUT_VAR PROTO_SRCS
    PROTOC_OUT_DIR "${GEN_DIR}"
    IMPORT_DIRS "${PROTO_DIR}"
    PROTOS "${PROTO_FILE}"
)

find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

set(GRPC_SRCS "${GEN_DIR}/accelerometer.grpc.pb.cc")
set(GRPC_HDRS "${GEN_DIR}/accelerometer.grpc.pb.h")

add_custom_command(
    OUTPUT ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out=${GEN_DIR} 
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE} 
         -I ${PROTO_DIR} 
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    VERBATIM
)

add_library(shared_proto
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_include_directories(shared_proto PUBLIC ${GEN_DIR} ${PROTO_DIR})
target_link_libraries(shared_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_subdirectory(server)
add_subdirectory(node_a)
add_subdirectory(node_b)