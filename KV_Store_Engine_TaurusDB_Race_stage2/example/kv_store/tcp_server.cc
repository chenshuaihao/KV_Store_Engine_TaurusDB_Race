#include "tcp_server.h"
#include <unistd.h>
#include <thread>
#include <memory>
#include <chrono>
#include <condition_variable>

#include "nanomsg/nn.h"
#include "nanomsg/reqrep.h"
#include "utils.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())


TcpServer::TcpServer() {
}

TcpServer::~TcpServer() {
    stopAll();
}

int TcpServer::Run(const char * url, std::shared_ptr<RpcProcess> rpc_process) {
    auto & inst = getInst();
    int fd = inst.start(url);

    if (fd != -1) {
        std::thread recv(&TcpServer::processRecv, fd, rpc_process);
        recv.detach();
    }

    return fd;
}

void TcpServer::Stop(int fd) {
    getInst().stop(fd);
}

void TcpServer::StopAll() {
    getInst().stopAll();
    sleep(1);
}

TcpServer & TcpServer::getInst() {
    static TcpServer server;
    return server;
}

int TcpServer::start(const char * url) {
    int fd = nn_socket(AF_SP, NN_REP);
    if (fd < 0) {
        NN_LOG(ERROR, "nn_socket");
        return -1;
    }

    if (nn_bind(fd, url) < 0) {
        NN_LOG(ERROR, "nn_bind with fd: " << fd);
        nn_close(fd);
        return -1;
    }

    mutex_.lock();
    fds_.emplace_back(fd);
    mutex_.unlock();

    KV_LOG(INFO) << "bind on " << url << " success with fd: " << fd;

    return fd;
}

void TcpServer::stop(int fd) {
    if (fd < 0)  {
        KV_LOG(ERROR) << "error with fd: " << fd;
        return;
    }

    mutex_.lock();
    auto it = std::find(fds_.begin(), fds_.end(), fd);
    if (it != fds_.end()) {
        mutex_.unlock();
        KV_LOG(ERROR) << "stop with fd: " << fd;
        return ;
    }
    fds_.erase(it);
    mutex_.unlock();

    nn_close(fd);
    KV_LOG(INFO) << "stop: " << fd;
}

void TcpServer::stopAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & fd : fds_) {
        fds_.clear();
    }
    fds_.clear();
}

void TcpServer::processRecv(int fd, std::shared_ptr<RpcProcess> process) {
    if (fd == -1 || process == nullptr) {
        return ;
    }

    std::mutex mtx;
    std::condition_variable cv;

    std::function<void (char *, int)> cb =
        [&] (char * buf, int len) {
            if (buf == nullptr || len < 0) {
                KV_LOG(ERROR) << "reply callback param error, buf is nullptr or len =" << len;
                return;
            }
            int rc = nn_send(fd, buf, len, 0);
            if (rc < 0) {
                NN_LOG(ERROR, "nn_send with fd: " << fd);
            } else {
                cv.notify_one();
            }
        };

    char * recv_buf;
    //local process no more than 50ms, or client will retry
    std::chrono::milliseconds duration(50); 

    while(1) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait_for(lock, duration);

        int rc = nn_recv(fd, &recv_buf, NN_MSG, 0);
        if (rc < 0) {
            NN_LOG(ERROR, "nn_recv with fd: " << fd);
            break;
        }

        char * buf = new char [rc];
        memcpy(buf, recv_buf, rc);
        nn_freemsg(recv_buf);
        process->Insert(buf, rc, cb);
    }
}

