# Protobuf code generation without relying on cmake's protobuf_generate module.
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(GEN_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(PROTO_FILES ${PROTO_DIR}/raft.proto ${PROTO_DIR}/kv.proto)
set(PROTO_GEN_SRCS ${GEN_DIR}/raft.pb.cc ${GEN_DIR}/kv.pb.cc)
set(PROTO_GEN_HDRS ${GEN_DIR}/raft.pb.h ${GEN_DIR}/kv.pb.h)

find_program(PROTOC_EXECUTABLE protoc REQUIRED)

add_custom_command(
  OUTPUT ${PROTO_GEN_SRCS} ${PROTO_GEN_HDRS}
  COMMAND ${PROTOC_EXECUTABLE}
          --proto_path=${PROTO_DIR}
          --cpp_out=${GEN_DIR}
          ${PROTO_FILES}
  DEPENDS ${PROTO_FILES}
  COMMENT "protoc: generating C++ from raft.proto and kv.proto"
  VERBATIM)

add_library(yewu_proto STATIC ${PROTO_GEN_SRCS})
target_include_directories(yewu_proto PUBLIC ${GEN_DIR})
target_link_libraries(yewu_proto PUBLIC protobuf::libprotobuf)
set_target_properties(yewu_proto PROPERTIES CXX_CLANG_TIDY "")
