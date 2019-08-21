#pragma once
#include <memory>
#include <mutex>

#include "kv_string.h"
#include "kv_intf.h"
#include "data_agent.h"
#include "KeyFile.h"
#include "asio/include/asio.hpp"
class KVService : public KVIntf, public std::enable_shared_from_this<KVService> {
public:
    bool Init(const char * host, int id);

    void Close();

    int Set(KVString &key, KVString & val);

    int Get(KVString &key, KVString & val);

    std::shared_ptr<KVService> GetPtr() {
        return shared_from_this();
    }

private:
    std::string host_;

    int id_;

    std::mutex mutex_;

    int ref_ = 0;

    KVString dir_;

    int no_;

    KeyFile *keyFile_;

    DataAgent* data_;
};

