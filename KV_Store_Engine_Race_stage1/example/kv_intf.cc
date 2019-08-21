#include "kv_intf.h"
#include "kv_store.h"

static std::shared_ptr<KVStore> kStore(new KVStore);

std::shared_ptr<KVIntf> GetKVIntf() {
    return kStore->GetPtr();;
}


