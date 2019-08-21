#include "meta_mgr.h"

bool MetaMgr::Init(const char * dir) {
    Release();

    dir_ = FileIo::Dir(dir);

    key_fh_.reset(new FileIo(GetPath(dir_, "kv_store.key").Buf()));

    RestoreMeta();
    return true;
}

void MetaMgr::Release() {
    key_fh_.reset();
    keys_map_.clear();
}

void MetaMgr::Set(const KVString &key, uint64_t pos) {
    keys_map_[key] = pos;
    KVString val(reinterpret_cast<char *>(&pos), sizeof(pos));
    key_fh_->Append(const_cast<KVString &>(key), val);
    key_fh_->Flush();
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
    while(key_fh_->ReadKV(pos, key, val) > int_size) {
        keys_map_[key] = *(uint64_t *)val.Buf();
        pos += (key.Size() + val.Size() + int_size * 2);
    }
    return keys_map_.size();
}

