add_library(yewu_net STATIC
  socket_ops.cc
  poller.cc
  channel.cc
  event_loop.cc
  event_loop_thread.cc
  event_loop_thread_pool.cc
  acceptor.cc
  tcp_connection.cc
  tcp_server.cc)
target_link_libraries(yewu_net PUBLIC yewu_common Threads::Threads)
