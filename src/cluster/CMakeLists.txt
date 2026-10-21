add_library(yewu_cluster STATIC server.cc state_machine.cc)
target_link_libraries(yewu_cluster PUBLIC yewu_raft yewu_rpc)
