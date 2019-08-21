#include "kv_intf.h"
#include "kv_service.h"

static std::shared_ptr<KVService> kStore(new KVService);

std::shared_ptr<KVIntf> GetKVIntf() {
    return kStore->GetPtr();;
}


