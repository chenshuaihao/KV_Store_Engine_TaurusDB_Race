#pragma once

#include <memory>

#include "kv_string.h"

class KVStoreIntf {
public:
    virtual ~KVStoreIntf() {};

    virtual bool Init(const char * dir, int id) = 0;

    virtual void Close() = 0;

    virtual int64_t Set(KVString &key, KVString & val) = 0;

    virtual int64_t Get(KVString &key, KVString & val) = 0;

    virtual int GetThreadId() const = 0;
};

extern "C" std::shared_ptr<KVStoreIntf> GetKVIntf();
