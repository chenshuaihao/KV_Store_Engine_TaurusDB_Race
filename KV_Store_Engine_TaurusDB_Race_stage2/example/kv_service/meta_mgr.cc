#include "meta_mgr.h"
#include "utils.h"

bool MetaMgr::Init(std::shared_ptr<DataAgent> & agent) {
    Release();

    agent_ = agent;

    RestoreMeta();
    return true;
}

void MetaMgr::Release() {
    keys_map_.clear();
    agent_.reset();
}

void MetaMgr::Set(const KVString &key, uint64_t pos) {
    keys_map_[key] = pos;
    KVString val(reinterpret_cast<char *>(&pos), sizeof(pos));
    agent_->Append(KV_OP_META_APPEND, const_cast<KVString &>(key), val);
}

uint64_t MetaMgr::Get(const KVString &key) {
    if (keys_map_.count(key) > 0) {
        return keys_map_[key];
    } else {
        return 0;
    }
}

int MetaMgr::RestoreMeta() {
    KVString key;
    KVString val;
    int pos = 0;
    const int int_size = sizeof(int);

    while(agent_->Get(KV_OP_META_GET, pos, key, val) > int_size) {
        keys_map_[key] = *(uint64_t *)val.Buf();
        pos += (key.Size() + val.Size() + int_size * 2);
    }
    return keys_map_.size();
}

