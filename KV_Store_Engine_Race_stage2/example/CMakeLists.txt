cmake_minimum_required(VERSION 2.8)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -DELPP_NO_DEFAULT_LOG_FILE -DELPP_THREAD_SAFE")

add_subdirectory(third_party)
add_subdirectory(kv_store)
add_subdirectory(kv_service)

