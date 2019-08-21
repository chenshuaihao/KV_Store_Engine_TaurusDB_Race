/*
    kvStoreEngine.h 存储引擎头文件，继承自基类KVIntf
*/

#ifndef KV_STORE_ENGINE_H_
#define KV_STORE_ENGINE_H_

#include <set>
#include <map>
#include <thread>

#include "kv_intf.h"
#include "KeyFile.h"
#include "ValueFile.h"
#include "params.h"

using namespace std;
class bufferFile;

//阶段类型
typedef enum
{
    ORDER_READ = 0, //顺序读
    RANDOM_READ //随机读
}StageType;

class KvStoreEngine : public KVIntf
{
public:
    KvStoreEngine(/* args */);
    ~KvStoreEngine();

    bool Init(const char * dir, int id);

    void Close();

    int Set(KVString &key, KVString & val);

    int Get(KVString &key, KVString & val);

    int GetThreadId() const{
        return threadId_;
    }
private:
    //目录
    std::string dir_;

    //tid
    int threadId_;

    //标识当前测试阶段
    StageType stagetype_;    

    //数据库引擎key、value文件
    KeyFile *keyFile_;
    ValueFile *valueFile_;
};


#endif
