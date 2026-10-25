add_library(yewu_raft STATIC election.cc log.cc file_store.cc node.cc persistence_queue.cc)
target_link_libraries(yewu_raft PUBLIC yewu_common Threads::Threads)
