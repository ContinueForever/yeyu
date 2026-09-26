add_executable(yewukv-server yewukv_server.cc)
target_link_libraries(yewukv-server PRIVATE yewu_kv yewu_rpc)

add_executable(yewukv-client yewukv_client.cc)
target_link_libraries(yewukv-client PRIVATE yewu_rpc)
