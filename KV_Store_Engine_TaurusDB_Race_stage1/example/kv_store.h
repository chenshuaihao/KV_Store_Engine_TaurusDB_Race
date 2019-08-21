#pragma once
#include <memory>
#include <mutex>

#include "kv_string.h"
#include "kv_intf.h"
#include "meta_mgr.h"
#include "data_mgr.h"

class KVStore : public KVIntf, public std::enable_shared_from_this<KVStore> {
public:
    bool Init(const char * dir, int id);

    void Close();

    int Set(KVString &key, KVString & val);

    int Get(KVString &key, KVString & val);

    std::shared_ptr<KVStore> GetPtr() {
        return shared_from_this();
    }

    int GetThreadId() const{ }

private:
    std::mutex mutex_;

    int ref_ = 0;

    KVString dir_;

    int no_;

    std::unique_ptr<MetaMgr> meta_ = std::unique_ptr<MetaMgr>(new MetaMgr);

    std::unique_ptr<DataMgr> data_ = std::unique_ptr<DataMgr>(new DataMgr);
};

