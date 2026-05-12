cmake_minimum_required(VERSION 3.20)
project(mini2_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wno-unused-parameter)

# Pick up Homebrew prefixes on macOS.
list(APPEND CMAKE_PREFIX_PATH
  /opt/homebrew/opt/grpc
  /opt/homebrew/opt/protobuf
  /opt/homebrew/opt/abseil
  /opt/homebrew
)

# Try CONFIG mode first (Homebrew on macOS ships these); fall back to MODULE
# mode on systems whose protobuf-devel only installs FindProtobuf.cmake
# (e.g. Fedora 39+).
find_package(Protobuf CONFIG QUIET)
if(NOT TARGET protobuf::libprotobuf)
  find_package(Protobuf REQUIRED)
  if(NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf INTERFACE IMPORTED)
    set_property(TARGET protobuf::libprotobuf PROPERTY
      INTERFACE_LINK_LIBRARIES ${Protobuf_LIBRARIES})
    set_property(TARGET protobuf::libprotobuf PROPERTY
      INTERFACE_INCLUDE_DIRECTORIES ${Protobuf_INCLUDE_DIRS})
  endif()
  if(NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc IMPORTED)
    set_property(TARGET protobuf::protoc PROPERTY
      IMPORTED_LOCATION ${Protobuf_PROTOC_EXECUTABLE})
  endif()
endif()
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

# OpenMP (Homebrew libomp on macOS). Optional — we have ifdef guards so the
# build still works without it (just runs the parallel sections single-threaded).
find_package(OpenMP)
if(NOT OpenMP_CXX_FOUND AND APPLE)
  # AppleClang doesn't auto-find Homebrew libomp; wire it up explicitly.
  set(_libomp_prefix "/opt/homebrew/opt/libomp")
  if(EXISTS "${_libomp_prefix}/include/omp.h")
    add_library(OpenMP::OpenMP_CXX INTERFACE IMPORTED)
    target_compile_options(OpenMP::OpenMP_CXX INTERFACE
      -Xpreprocessor -fopenmp -I${_libomp_prefix}/include)
    target_link_libraries(OpenMP::OpenMP_CXX INTERFACE
      ${_libomp_prefix}/lib/libomp.dylib)
    set(OpenMP_CXX_FOUND TRUE)
    message(STATUS "Using Homebrew libomp at ${_libomp_prefix}")
  endif()
endif()

# --- codegen --------------------------------------------------------------
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
set(GEN_DIR   ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN_DIR})

set(PROTO_FILE ${PROTO_DIR}/mini2.proto)
set(PROTO_SRCS ${GEN_DIR}/mini2.pb.cc ${GEN_DIR}/mini2.grpc.pb.cc)
set(PROTO_HDRS ${GEN_DIR}/mini2.pb.h  ${GEN_DIR}/mini2.grpc.pb.h)

get_target_property(GRPC_CPP_PLUGIN_EXECUTABLE gRPC::grpc_cpp_plugin LOCATION)
get_target_property(PROTOC_EXECUTABLE protobuf::protoc LOCATION)

add_custom_command(
  OUTPUT  ${PROTO_SRCS} ${PROTO_HDRS}
  COMMAND ${PROTOC_EXECUTABLE}
          --proto_path=${PROTO_DIR}
          --cpp_out=${GEN_DIR}
          --grpc_out=${GEN_DIR}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
          ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  VERBATIM
)

add_library(mini2_proto STATIC ${PROTO_SRCS})
target_include_directories(mini2_proto PUBLIC ${GEN_DIR})
target_link_libraries(mini2_proto PUBLIC
  gRPC::grpc++
  protobuf::libprotobuf
)

# --- common helpers -------------------------------------------------------
add_library(mini2_common STATIC
  common/overlay.cpp
  common/csv_store.cpp
  common/scheduler.cpp
  common/telemetry.cpp
  common/routing.cpp
)
target_include_directories(mini2_common PUBLIC common)
target_link_libraries(mini2_common PUBLIC mini2_proto Threads::Threads)
if(OpenMP_CXX_FOUND)
  target_link_libraries(mini2_common PUBLIC OpenMP::OpenMP_CXX)
endif()

# --- node daemon (server) -------------------------------------------------
add_executable(mini2_node server/node.cpp)
target_link_libraries(mini2_node PRIVATE mini2_common)

# --- client ---------------------------------------------------------------
add_executable(mini2_client client/client.cpp)
target_link_libraries(mini2_client PRIVATE mini2_common)
