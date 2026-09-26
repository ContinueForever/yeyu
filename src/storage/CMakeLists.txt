add_library(yewu_storage STATIC mem_engine.cc executor.cc wal_engine.cc)
target_link_libraries(yewu_storage PUBLIC yewu_common)
