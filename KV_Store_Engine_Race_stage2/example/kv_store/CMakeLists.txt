cmake_minimum_required(VERSION 2.8)
project(example_store)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -DELPP_NO_DEFAULT_LOG_FILE -DELPP_THREAD_SAFE")

set(SRC_BASE ${PROJECT_SOURCE_DIR})
include_directories(
    ${SRC_BASE}/../../third_party
    ${SRC_BASE}/../third_party
    ${SRC_BASE}/../../src/include
    ${SRC_BASE}/../include
    )

link_directories(
    ${SRC_BASE}/../third_party/nanomsg
    )

set(SRC_KVSTORE
    ${SRC_BASE}/kv_store.cc
    ${SRC_BASE}/file_io.cc
    ${SRC_BASE}/data_mgr.cc
    ${SRC_BASE}/tcp_server.cc
    ${SRC_BASE}/rpc_process.cc
    )

add_executable(${PROJECT_NAME} ${SRC_KVSTORE})
target_link_libraries(${PROJECT_NAME} libnanomsg.a pthread anl)
