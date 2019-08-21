#ifndef __HUAWEI_UTILS_H__
#define __HUAWEI_UTILS_H__
//////////////////////////////////////////////////////////////////////////////////////////////////
#include "easylogging++.h"

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
        uint32_t cnt = 0;
        for (int i = 0; i < len; i ++) {
            cnt += (uint8_t)buf[i];
        }
        return cnt + len + sn + type;
    }
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////////////////////////////////
#endif
