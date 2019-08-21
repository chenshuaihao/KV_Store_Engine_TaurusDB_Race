#include "tcp_client.h"
#include <unistd.h>
#include <thread>
#include <memory>

#include "nanomsg/nn.h"
#include "nanomsg/reqrep.h"
#include "utils.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())


TcpClient::TcpClient() {
}

TcpClient::~TcpClient() {
    closeAll();
}

int TcpClient::Connect(const char * url) {
    auto & inst = getInst();
    int fd = inst.connect(url);

    return fd;
}

void TcpClient::Close(int fd) {
    getInst().close(fd);
}

void TcpClient::CloseAll() {
    getInst().closeAll();
    sleep(1);
}

char * TcpClient::Send(int fd, char * buf, int len) {
    int rc = nn_send(fd, buf, len, 0);
    if (rc < 0) {
        NN_LOG(ERROR, "nn_send") << "ret: " << rc;
        return nullptr;
    }

    char *msg;
    rc = nn_recv(fd, &msg, NN_MSG, 0);
    if (rc < 0) {
        NN_LOG(ERROR, "nn_recv with NN_MSG") << "ret: " << rc;
        return nullptr;
    }

    char * ret = nullptr;
    do {
        if (rc < PACKET_HEADER_SIZE) {
            KV_LOG(ERROR) << "recv msg size is less than packet header: " << rc;
            break;
        }

        auto & packet = *(Packet *) msg;
        if (rc < PACKET_HEADER_SIZE + packet.len) {
            KV_LOG(ERROR) << "recv msg size is less than expect: " << rc << "-" << packet.len;
            break;
        }

        int sum = packet.Sum();
        if (packet.crc != sum) {
            KV_LOG(ERROR) << "recv msg crc error, expect: " << packet.crc << " get: " << sum;
            break;
        }

        ret = new char[rc];
        memcpy(ret, msg, rc);
    } while(0);

    nn_freemsg(msg);
    return ret;
}

TcpClient & TcpClient::getInst() {
    static TcpClient server;
    return server;
}

int TcpClient::connect(const char * url) {
    int fd = nn_socket(AF_SP, NN_REQ);
    if (fd < 0) {
        NN_LOG(ERROR, "nn_socket");
        return -1;
    }

    if (nn_connect(fd, url) < 0) {
        NN_LOG(ERROR, "nn_connect");
        nn_close(fd);
        return -1;
    }

    mutex_.lock();
    fds_.emplace_back(fd);
    mutex_.unlock();

    KV_LOG(INFO) << "connect to store node success. fd: " << fd;

    return fd;
}

void TcpClient::close(int fd) {
    if (fd < 0)  {
        KV_LOG(ERROR) << "error fd: " << fd;
        return;
    }

    mutex_.lock();
    auto it = std::find(fds_.begin(), fds_.end(), fd);
    if (it == fds_.end()) {
        mutex_.unlock();
        KV_LOG(ERROR) << "stop unknown fd: " << fd;
        return ;
    }
    fds_.erase(it);
    mutex_.unlock();

    nn_close(fd);
    KV_LOG(INFO) << "stop: " << fd;
}

void TcpClient::closeAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & fd : fds_) {
        nn_close(fd);
    }
    fds_.clear();
}

