add_library(yewu_raft STATIC election.cc log.cc)
target_link_libraries(yewu_raft PUBLIC yewu_common)
