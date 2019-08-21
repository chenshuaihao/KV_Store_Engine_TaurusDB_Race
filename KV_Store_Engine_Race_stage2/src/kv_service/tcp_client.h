#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>
#include "asio/include/asio.hpp"
using asio::ip::tcp;

class TcpClient {
public:
    TcpClient();
    ~TcpClient();

    int Connect(const char * host, const char* port);

    void Close();

    char * Send(char * buf, int len);

    int SendOnly(char * buf, int len);

    int Recv(char * buf, int len);

protected:
    asio::io_context io_context;

    tcp::socket s;

private:
    bool isConnected_;
    //std::shared_ptr<RpcProcess> process_;
};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

