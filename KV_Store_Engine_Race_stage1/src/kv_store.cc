#include "kv_intf.h"
#include "KvStoreEngine.h"

std::shared_ptr<KVIntf> GetKVIntf() {
    return std::make_shared<KvStoreEngine>();
}
/*
static std::shared_ptr<KvStoreEngine> kvStoreEngine(new KvStoreEngine);

std::shared_ptr<KVIntf> GetKVIntf() {
    return kvStoreEngine->GetPtr();;
}
*/
