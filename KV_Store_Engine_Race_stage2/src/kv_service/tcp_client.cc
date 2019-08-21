#include "tcp_client.h"
#include <unistd.h>
#include <thread>
#include <memory>


//#include "nanomsg-1.1.5/src/nn.h"
//#include "nanomsg-1.1.5/src/reqrep.h"
//#include "nanomsg-1.1.5/src/pair.h"
#include "utils.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: "

using asio::ip::tcp;

TcpClient::TcpClient()
    : io_context(), s(io_context), isConnected_(false)
{
}

TcpClient::~TcpClient() {
    this->Close();
    //closeAll();
}

int TcpClient::Connect(const char * host, const char* port) {
    if (isConnected_) {
        return 0;
    }
    KV_LOG(INFO) << "TcpClient::Connect\n";
    tcp::resolver resolver(io_context);
    asio::connect(s, resolver.resolve(host, port));
    isConnected_ = true;
    return 0;
    //return fd;
}

void TcpClient::Close() {
    s.close();
    this->isConnected_ = false;
}

char * TcpClient::Send(char * buf, int len) {    
    asio::error_code error;
    try
    {
        size_t rc = s.write_some(asio::buffer(buf, len), error);//asio::write()
        if (error == asio::error::eof) {
            KV_LOG(ERROR) << "write_some:" << error.message();
            return nullptr; // Connection closed cleanly by peer.
        }            
        else if (error)
            throw asio::system_error(error); // Some other error.

        if (unlikely(rc < 0)) {
            NN_LOG(ERROR, "asio::write") << "ret: " << rc;
            return nullptr;
        }

        char *reply = new char[8192];
        rc = s.read_some(asio::buffer(reply, 8192), error);
        if (error == asio::error::eof) {
            KV_LOG(ERROR) << "read_some:" << error.message();
            return nullptr; // Connection closed cleanly by peer.
        }            
        else if (error)
            throw asio::system_error(error); // Some other error.

        if (unlikely(rc < 0)) {
            NN_LOG(ERROR, "asio::write with NN_MSG") << " ret: " << rc;
            return nullptr;
        }

        return reply;
    }
    catch(const std::exception& e)
    {
        //printf("error write or read:%s\n", e.what());
        KV_LOG(ERROR) << "TcpClient::Send error:" << e.what();
        return nullptr;
    }    
}

//only send data
int TcpClient::SendOnly(char * buf, int len) {    
    asio::error_code error;
    try
    {
        size_t rc = asio::write(s, asio::buffer(buf, len), error);//asio::write()
//        size_t rc = s.write_some(asio::buffer(buf, len),error);//asio::write()
        if (error == asio::error::eof) {
            KV_LOG(ERROR) << "asio::write:" << error.message();
            return -1; // Connection closed cleanly by peer.
        }            
        else if (error)
            throw asio::system_error(error); // Some other error.
        return rc;

    }
    catch(const std::exception& e)
    {
        //printf("error write or read:%s\n", e.what());
        KV_LOG(ERROR) << "TcpClient::SendOnly error:" << e.what();
        return -1;
    }    
}

// only recv data
int TcpClient::Recv(char * buf, int len) {    
    asio::error_code error;
    try
    {
        size_t rc = asio::read(s, asio::buffer(buf, len), error);
//        size_t rc = s.read_some(asio::buffer(buf, len), error);
        if (error == asio::error::eof) {
            KV_LOG(ERROR) << "asio::read:" << error.message();
            return -1; // Connection closed cleanly by peer.
        }            
        else if (error)
            throw asio::system_error(error); // Some other error.
        return rc;
    }
    catch(const std::exception& e)
    {
        //printf("error write or read:%s\n", e.what());
        KV_LOG(ERROR) << "TcpClient::Recv error:" << e.what();
        return -1;
    }
}

