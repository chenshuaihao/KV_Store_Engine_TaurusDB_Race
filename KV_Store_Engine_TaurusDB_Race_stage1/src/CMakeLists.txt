CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(kv_store)

INCLUDE_DIRECTORIES(${SRC_BASE}/../third_party)
SET(CMAKE_CXX_FLAGS "-std=c++11  -pthread -O2")
SET(SRC_BASE ${PROJECT_SOURCE_DIR})
SET(SRC_CPP ${SRC_BASE}/kv_store.cc 
            ${SRC_BASE}/KvStoreEngine.cc
            ${SRC_BASE}/KeyFile.cc
            ${SRC_BASE}/ValueFile.cc
)

ADD_LIBRARY(${PROJECT_NAME} SHARED ${SRC_CPP})

