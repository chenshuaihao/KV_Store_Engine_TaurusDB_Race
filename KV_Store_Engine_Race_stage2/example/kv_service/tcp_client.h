#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>

class TcpClient {
public:
    TcpClient();
    ~TcpClient();

    static int Connect(const char * url);

    static void Close(int fd);

    static void CloseAll();

    static char * Send(int fd, char * buf, int len);
protected:

    static TcpClient & getInst();

    int connect(const char * url);

    void close(int fd);

    void closeAll();

protected:
    std::mutex mutex_;
    std::vector<int> fds_;

    //std::shared_ptr<RpcProcess> process_;
};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

