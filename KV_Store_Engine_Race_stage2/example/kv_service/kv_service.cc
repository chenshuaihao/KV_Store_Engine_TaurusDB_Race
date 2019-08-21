#include "kv_service.h"
#include "utils.h"

bool KVService::Init(const char * host, int id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ref_ ++ == 0) {
        char url[256];
        strcpy(url, host);
        strcat(url, ":9527");
        data_->Init(url);
        meta_->Init(data_);
    }
    return true;
}

void KVService::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if ( ref_ > 0 && (-- ref_) == 0) {
        data_->Release();
        meta_->Release();
    }
}

int KVService::Set(KVString & key, KVString & val) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t pos = data_->Append(KV_OP_DATA_APPEND, key, val);
    int ret = 0;
    meta_->Set(key, pos);

    return (pos >> 32);
}

int KVService::Get(KVString & key, KVString & val) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t pos = meta_->Get(key);
    data_->Get(KV_OP_DATA_GET, pos, key, val);
    return (pos >> 32);
}
