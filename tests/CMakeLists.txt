add_executable(yewu_tests
  test_buffer.cc
  test_tcp_connection.cc
  test_frame_codec.cc
  test_rpc_client.cc
  test_text_protocol.cc
  test_mem_engine.cc
  test_raft_election.cc
  test_raft_log.cc
  test_raft_file_store.cc
  test_raft_node.cc
  test_raft_snapshot.cc
  test_cluster_state_machine.cc
  test_wal_engine.cc
  test_storage_executor.cc
  test_storage_server.cc)
target_link_libraries(yewu_tests PRIVATE
  yewu_kv
  yewu_rpc
  yewu_raft
  yewu_cluster
  GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(yewu_tests PROPERTIES TIMEOUT 30)

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

add_test(NAME cluster_failover_recovery
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_cluster.py
          --node $<TARGET_FILE:yewukv-node> --client $<TARGET_FILE:yewukv-client>)
set_tests_properties(cluster_failover_recovery PROPERTIES TIMEOUT 90)

add_test(NAME linearizability_checker
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_linearizability.py)
set_tests_properties(linearizability_checker PROPERTIES TIMEOUT 10)

add_test(NAME cluster_backup_manifest
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_cluster_backup.py)
set_tests_properties(cluster_backup_manifest PROPERTIES TIMEOUT 10)

add_library(yewu_test_io_fault SHARED test_io_fault.cc)
target_link_libraries(yewu_test_io_fault PRIVATE ${CMAKE_DL_LIBS})
target_compile_options(yewu_test_io_fault PRIVATE -fno-sanitize=all)
target_link_options(yewu_test_io_fault PRIVATE -fno-sanitize=all)
set(snapshot_test_args --node $<TARGET_FILE:yewukv-node> --client $<TARGET_FILE:yewukv-client>)
add_test(NAME cluster_snapshot_runtime
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_cluster_snapshot.py
          ${snapshot_test_args} --suite snapshot)
set_tests_properties(cluster_snapshot_runtime PROPERTIES TIMEOUT 240)
set(resource_test_args --fault-library $<TARGET_FILE:yewu_test_io_fault>)
if(YEWUKV_ENABLE_SANITIZERS)
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=libasan.so
                  OUTPUT_VARIABLE asan_runtime OUTPUT_STRIP_TRAILING_WHITESPACE)
  list(APPEND resource_test_args --asan-runtime ${asan_runtime})
endif()
add_test(NAME cluster_resource_runtime
  COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_cluster_snapshot.py
          ${snapshot_test_args} --suite resource ${resource_test_args})
set_tests_properties(cluster_resource_runtime PROPERTIES TIMEOUT 120)

add_test(NAME cluster_fault_campaign_smoke
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/scripts/cluster-soak.py
          ${snapshot_test_args} --epochs 5
          --evidence-root ${CMAKE_CURRENT_BINARY_DIR}/soak-evidence)
set_tests_properties(cluster_fault_campaign_smoke PROPERTIES TIMEOUT 120)
