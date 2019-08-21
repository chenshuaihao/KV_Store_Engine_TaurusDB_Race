#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>
#include "rpc_process.h"

class TcpServer {
public:
    TcpServer();
    ~TcpServer();

    static int Run(const char * url, std::shared_ptr<RpcProcess> process);

    static void Stop(int fd);

    static void StopAll();
protected:

    static TcpServer & getInst();

    int start(const char * url);

    void stop(int fd);

    void stopAll();

    static void processRecv(int fd, std::shared_ptr<RpcProcess> process);

protected:
    std::mutex mutex_;

    std::vector<int> fds_;
};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

