#ifndef __HUAWEI_RPC_SERVICE_H__
#define __HUAWEI_RPC_SERVICE_H__
/////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <list>
#include <memory>
#include <atomic>
#include <condition_variable>

#include "data_mgr.h"
#include "kv_string.h"

typedef std::function<void (char *, int)> DoneCbFunc;

struct PacketInfo {
    char * buf      = nullptr;
    DoneCbFunc cb   = nullptr;

    PacketInfo(char * buf, DoneCbFunc cb)
    : buf(buf), cb(cb) {
    }
};


class RpcProcess : public std::enable_shared_from_this<RpcProcess> {
public:
    RpcProcess(const char * dir)
    : dir_(dir), run_(false) {
    }

    ~RpcProcess() {
        Stop();
    }

    bool Insert(char * buf, int len, DoneCbFunc cb);

    bool Run(const char * dir, bool clear);

    bool IsRun() {
        return run_;
    }

    void Stop();

    std::shared_ptr<RpcProcess> GetPtr() {
        return shared_from_this();
    }

protected:
    bool process();

    void processAppend(DataMgr & target, char * buf, DoneCbFunc cb);

    void processGet(DataMgr & target, char * buf, DoneCbFunc cb);

protected:
    KVString            dir_;
    std::atomic_bool    run_;
    DataMgr             data_;
    DataMgr             meta_;

    std::mutex              mutex_;
    std::condition_variable cv_;
    std::list<PacketInfo>   reqQ_;
};
/////////////////////////////////////////////////////////////////////////////////////////////
#endif
