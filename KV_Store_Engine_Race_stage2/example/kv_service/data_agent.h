#pragma once

#include <cstdint>
#include <map>
#include <memory>

#include "kv_string.h"

class DataAgent {
public:
    ~DataAgent();

    int Init(const char * url);

    void Release();

    void Clear();

    uint64_t Append(uint32_t type, KVString & key, KVString & val);

    int Get(uint32_t type, uint64_t pos, KVString & key, KVString &val);
protected:
    int fd_ = -1;
};
