#ifndef __HUAWEI_UTILS_H__
#define __HUAWEI_UTILS_H__
//////////////////////////////////////////////////////////////////////////////////////////////////
#include <stdio.h>
#define ELPP_DISABLE_LOGS //日志开关，默认关，注释这一句即开启日志
#include "easylogging++.h"

#define likely(x) __builtin_expect(!!(x), 1) //gcc内置函数, 帮助编译器分支优化
#define unlikely(x) __builtin_expect(!!(x), 0)

#define KV_LOG(level) LOG(level) << "[" << __FUNCTION__ << ":" << __LINE__ << "] "

const int KV_OP_META_APPEND = 1;
const int KV_OP_META_GET    = 2;
const int KV_OP_DATA_APPEND = 3;
const int KV_OP_DATA_GET    = 4;
const int KV_OP_CLEAR       = 5;

#pragma pack(push)
#pragma pack(1)
const int PACKET_HEADER_SIZE = sizeof(int32_t) * 4;
struct Packet {
    uint32_t len = 0;
    uint32_t crc = 0;
    uint32_t sn  = 0;
    uint32_t type= 0;
    char buf[0];

    uint32_t Sum() {
        return 0;
    }
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////////////////////////////////
#endif
