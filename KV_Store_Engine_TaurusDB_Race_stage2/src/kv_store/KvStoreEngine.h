/*
    kvStoreEngine.h 存储引擎头文件，继承自基类KVIntf
*/

#ifndef KV_STORE_ENGINE_H_
#define KV_STORE_ENGINE_H_

#include <set>
#include <map>
#include <thread>
#include <memory>
#include <functional>
#include "kv_store_intf.h"
#include "KeyFile.h"
#include "ValueFile.h"
#include "params.h"

using namespace std;
class bufferFile;

typedef std::function<void (char *, int)> DoneCbFunc;

//阶段类型
typedef enum
{
    ORDER_READ = 0, //顺序读
    RANDOM_READ //随机读
}StageType;

class KvStoreEngine : public KVStoreIntf, public std::enable_shared_from_this<KvStoreEngine>
{
public:
    KvStoreEngine(/* args */);
    ~KvStoreEngine();

    bool Init(const char * dir, int id);

    void Close();

    int64_t Set(KVString &key, KVString & val);

    int64_t Get(KVString &key, KVString & val);

    int64_t Get(int64_t keyPosition, KVString & val, int state);

    int GetThreadId() const{
        return threadId_;
    }

    size_t Process(char * buf, int len);

    size_t ProcessAppend(char * buf);

    size_t ProcessGet(char * buf);

    size_t ProcessGetKey(char * buf);

    size_t ProcessUnknown(char * buf , int len);

    std::shared_ptr<KvStoreEngine> GetPtr() {
        return shared_from_this();
    }
private:
    //目录
    std::string dir_;

    //tid
    int threadId_;

    int64_t count_;
    
    //标识当前测试阶段
    StageType stagetype_;    

    //数据库引擎key、value文件
    KeyFile *keyFile_;
    ValueFile *valueFile_;
};


#endif
