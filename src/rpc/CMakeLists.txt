add_library(yewu_rpc STATIC
  frame_codec.cc
  kv_server.cc
  client.cc)
target_link_libraries(yewu_rpc PUBLIC yewu_net yewu_storage yewu_proto Threads::Threads)
