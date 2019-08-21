#include "kv_store.h"

bool KVStore::Init(const char * dir, int id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ref_ ++ == 0) {
        data_->Init(dir);
        meta_->Init(dir);
    }
    return true;
}

void KVStore::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if ( ref_ > 0 && (-- ref_) == 0) {
        data_->Release();
        meta_->Release();
    }
}

int KVStore::Set(KVString & key, KVString & val) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t pos = data_->Append(key, val);
    int ret = 0;
    meta_->Set(key, pos);

    return (pos >> 32);
}

int KVStore::Get(KVString & key, KVString & val) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t pos = meta_->Get(key);
    data_->Get(pos, key, val);
    return (pos >> 32);
}
