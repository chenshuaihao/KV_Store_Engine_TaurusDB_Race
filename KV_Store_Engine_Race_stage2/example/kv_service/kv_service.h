#pragma once
#include <memory>
#include <mutex>

#include "kv_string.h"
#include "kv_intf.h"
#include "meta_mgr.h"
#include "data_agent.h"

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
    std::mutex mutex_;

    int ref_ = 0;

    KVString dir_;

    int no_;

    std::unique_ptr<MetaMgr> meta_ = std::unique_ptr<MetaMgr>(new MetaMgr);

    std::shared_ptr<DataAgent> data_ = std::unique_ptr<DataAgent>(new DataAgent);
};

