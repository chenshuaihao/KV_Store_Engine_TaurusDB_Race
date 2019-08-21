cmake_minimum_required(VERSION 2.8)
project(kv_store)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -DELPP_NO_DEFAULT_LOG_FILE -DELPP_THREAD_SAFE")

set(SRC_BASE ${PROJECT_SOURCE_DIR})
include_directories(
    ${SRC_BASE}/../include
    ${SRC_BASE}/../third_party
    ${SRC_BASE}/../../third_party
    ${SRC_BASE}/../third_party/asio/include
    )

link_directories(
    ${SRC_BASE}/../third_party/nanomsg
    )

set(SRC_KVSTORE
    ${SRC_BASE}/kv_store_intf.cc
    ${SRC_BASE}/kv_store.cc
    ${SRC_BASE}/KvStoreEngine.cc
    ${SRC_BASE}/KeyFile.cc
    ${SRC_BASE}/ValueFile.cc
    ${SRC_BASE}/tcp_server.cc
    )

add_executable(${PROJECT_NAME} ${SRC_KVSTORE})
target_link_libraries(${PROJECT_NAME} pthread anl)

