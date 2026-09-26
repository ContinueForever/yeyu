add_executable(yewu_tests
  test_buffer.cc
  test_tcp_connection.cc
  test_frame_codec.cc
  test_rpc_client.cc
  test_text_protocol.cc
  test_mem_engine.cc
  test_raft_election.cc
  test_raft_log.cc
  test_wal_engine.cc
  test_storage_executor.cc
  test_storage_server.cc)
target_link_libraries(yewu_tests PRIVATE
  yewu_kv
  yewu_rpc
  yewu_raft
  GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(yewu_tests)

find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_test(NAME network_integration
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_network.py
          --server $<TARGET_FILE:yewukv-server>)
set_tests_properties(network_integration PROPERTIES TIMEOUT 120)

add_test(NAME network_rpc_integration
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_rpc_network.py
          --server $<TARGET_FILE:yewukv-server>)
set_tests_properties(network_rpc_integration PROPERTIES TIMEOUT 120)

add_test(NAME network_idle_integration
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_idle_network.py
          --server $<TARGET_FILE:yewukv-server>)
set_tests_properties(network_idle_integration PROPERTIES TIMEOUT 120)

add_test(NAME wal_process_recovery
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_wal_recovery.py
          --server $<TARGET_FILE:yewukv-server>)
set_tests_properties(wal_process_recovery PROPERTIES TIMEOUT 30)

add_test(NAME benchmark_accounting
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_benchmark.py)
set_tests_properties(benchmark_accounting PROPERTIES TIMEOUT 10)
