#include "tcp_server.h"
#include <unistd.h>
#include <thread>
#include <memory>
#include <chrono>
#include <condition_variable>
#include "asio/include/asio.hpp"
//#include "nanomsg/nn.h"
//#include "nanomsg/reqrep.h"
//#include "nanomsg-1.1.5/src/nn.h"
//#include "nanomsg-1.1.5/src/reqrep.h"
//#include "nanomsg-1.1.5/src/pair.h"
#include "../include/utils.h"
#include "KvStoreEngine.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: "


TcpServer::TcpServer(short port, std::shared_ptr<KvStoreEngine> kvStoreEngine)
    : io_context_(),
    acceptor_(io_context_, tcp::endpoint(tcp::v4(), port)),
    kvStoreEngine_(kvStoreEngine)
{
    do_accept();
}

TcpServer::~TcpServer() {
    //stopAll();
}

void TcpServer::do_accept()
{
    acceptor_.async_accept(
        [this](std::error_code ec, tcp::socket socket)
        {
        if (!ec)
        {
            KV_LOG(INFO) << "new client:" << socket.remote_endpoint().address() << ":" << socket.remote_endpoint().port();
            std::make_shared<session>(std::move(socket), kvStoreEngine_)->start();
        }

        do_accept();
    });
}

void TcpServer::Run() {
    io_context_.run();
}