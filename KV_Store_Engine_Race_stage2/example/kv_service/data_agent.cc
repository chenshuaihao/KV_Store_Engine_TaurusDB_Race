#include "data_agent.h"

#include "utils.h"
#include "tcp_client.h"

DataAgent::~DataAgent() {
    Release();
}

int DataAgent::Init(const char * url) {
    fd_ = TcpClient::Connect(url);
    return fd_;
}

void DataAgent::Release( ) {
    if (fd_ != -1) {
        TcpClient::Close(fd_);
        fd_ = -1;
    }
}

void DataAgent::Clear() {
}

uint64_t DataAgent::Append(uint32_t type, KVString & key, KVString & val) {
    int key_size = key.Size();
    int val_size = val.Size();

    int send_len = key_size + val_size + sizeof(int) * 2;
    char * send_buf = new char [PACKET_HEADER_SIZE + send_len];
    auto & send_pkt = *(Packet *)send_buf;
    memcpy(send_pkt.buf, (char *)&key_size, sizeof(int));
    memcpy(send_pkt.buf + sizeof(int), (char *)&val_size, sizeof(int));
    memcpy(send_pkt.buf + sizeof(int) * 2, key.Buf(), key_size);
    memcpy(send_pkt.buf + sizeof(int) * 2 + key_size, val.Buf(), val_size);
    send_pkt.len    = send_len;
    send_pkt.sn     = 0;
    send_pkt.type   = type;
    send_pkt.crc    = send_pkt.Sum();

    char * ret_buf = TcpClient::Send(fd_, send_buf, send_len + PACKET_HEADER_SIZE);
    delete [] send_buf;

    if (ret_buf == nullptr) {
        KV_LOG(ERROR) << "Append return null";
        return -1;
    }

    auto & recv_pkt = *(Packet *)ret_buf;
    auto ret = *(uint64_t *)recv_pkt.buf;
    delete [] ret_buf;
    return ret;
}

int DataAgent::Get(uint32_t type, uint64_t pos, KVString & key, KVString &val) {
    int send_len = sizeof(uint64_t);
    char * send_buf = new char[PACKET_HEADER_SIZE + send_len];
    auto & send_pkt = *(Packet *)send_buf;
    memcpy(send_pkt.buf, (char *)&pos, send_len);

    send_pkt.len    = send_len;
    send_pkt.sn     = 0;
    send_pkt.type   = type;
    send_pkt.crc = send_pkt.Sum();

    char * ret_buf = TcpClient::Send(fd_, send_buf, send_len + PACKET_HEADER_SIZE);
    delete [] send_buf;

    if (ret_buf == nullptr) {
        KV_LOG(ERROR) << "Get return null";
        return -1;
    }

    auto & recv_pkt = *(Packet *)ret_buf;
    int key_size = *(int *)recv_pkt.buf;
    int val_size = *(int *)(recv_pkt.buf + sizeof(int));

    char * k = new char [key_size];
    char * v = new char [val_size];
    memcpy(k, recv_pkt.buf + sizeof(int) * 2, key_size);
    memcpy(v, recv_pkt.buf + sizeof(int) * 2 + key_size, val_size);
    delete [] ret_buf;

    key.Reset(k, key_size);
    val.Reset(v, val_size);

    return key_size + val_size;
}

