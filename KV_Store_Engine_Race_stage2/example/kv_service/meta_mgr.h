#pragma once

#include <map>
#include <memory>

#include "kv_string.h"
#include "data_agent.h"

class MetaMgr {
public:
    bool Init(std::shared_ptr<DataAgent> & agent);

    void Release();

    void Set(const KVString & key, uint64_t pos);

    uint64_t Get(const KVString & key);

protected:
    int RestoreMeta();

    static KVString GetPath(KVString &dir, const char * name) {
        KVString path = dir;
        path = path + name;
        return path;
    }

private:
    std::map<KVString, uint64_t> keys_map_;

    std::shared_ptr<DataAgent> agent_;

    KVString dir_;
};
