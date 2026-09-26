add_library(yewu_kv STATIC
  text_protocol.cc
  server.cc)
target_link_libraries(yewu_kv
  PUBLIC yewu_net yewu_storage)
