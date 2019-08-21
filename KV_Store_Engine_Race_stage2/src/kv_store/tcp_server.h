#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>
#include <functional>
#include "asio/include/asio.hpp"
#include "KvStoreEngine.h"
//#include "asio-asio-1-13-0/asio/include/asio.hpp"

using asio::ip::tcp;

class session
  : public std::enable_shared_from_this<session>
{
public:
  session(tcp::socket socket, std::shared_ptr<KvStoreEngine> kvStoreEngine)
    : socket_(std::move(socket)),
    kvStoreEngine_(kvStoreEngine)
  {
  }

  void start()
  {
    do_read();
  }

private:
    void do_read()
    {
        auto self(shared_from_this());
        socket_.async_read_some(asio::buffer(data_, max_length),
            [this, self](std::error_code ec, std::size_t length)
            {
                if (!ec)
                {
                    size_t ret_len = kvStoreEngine_->Process(data_, length);
                    do_write(ret_len);
                }
            });
    }

    void do_write(std::size_t length)
    {
        auto self(shared_from_this());
        asio::async_write(socket_, asio::buffer(data_, length),
        [this, self](std::error_code ec, std::size_t /*length*/)
        {
          if (!ec)
          {
            do_read();
          }
        });
    }

    tcp::socket socket_;
    std::shared_ptr<KvStoreEngine> kvStoreEngine_;
    enum { max_length = 8192 };
    char data_[max_length];
};

class TcpServer {
public:
    TcpServer(short port, std::shared_ptr<KvStoreEngine> kvStoreEngine);
    ~TcpServer();

    void Run();


protected:

    void do_accept();

protected:
    asio::io_context io_context_;

    tcp::acceptor acceptor_;

    std::shared_ptr<KvStoreEngine> kvStoreEngine_;
};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

