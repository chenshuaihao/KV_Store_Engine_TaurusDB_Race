#include "rpc_process.h"

#include <unistd.h>
#include <thread>
#include "utils.h"

bool RpcProcess::Insert(char * buf, int len, DoneCbFunc cb) {
    if (buf == nullptr || len < PACKET_HEADER_SIZE) {
        KV_LOG(ERROR) << "insert to RpcProcess failed. size: " << len;
        return false;
    }
    auto & rpc = *(Packet *)buf;
    if (rpc.len + PACKET_HEADER_SIZE != len) {
        KV_LOG(ERROR) << "expect size: " << rpc.len << " buf size: " << len - PACKET_HEADER_SIZE;
        return false;
    }

    uint32_t sum = rpc.Sum();
    if (sum != rpc.crc) {
        KV_LOG(ERROR) << "crc error, expect: " << rpc.crc << " get: " << sum;
        return false;
    } else {
        std::lock_guard<std::mutex> lock(mutex_);
        reqQ_.emplace_back(buf, cb);
        cv_.notify_one();
        return true;
    }
}

bool RpcProcess::Run(const char * dir, bool clear) {
    const char * data_ext = ".data";
    const char * meta_ext = ".meta";

    dir_ = dir;
    data_.Init(dir_.Buf(), data_ext);
    meta_.Init(dir_.Buf(), meta_ext);

    if (clear) {
        data_.Clear();
        meta_.Clear();

        data_.Init(dir_.Buf(), data_ext);
        meta_.Init(dir_.Buf(), meta_ext);
    }

    std::thread th(&RpcProcess::process, this);
    th.detach();
}

void RpcProcess::Stop() {
    run_ = false;
    sleep(1);
}

bool RpcProcess::process() {
    run_ = true;
    while(run_) {
        std::list<PacketInfo> packets;
        do {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] ()->bool { return !reqQ_.empty() || !run_; });
            packets = std::move(reqQ_);
        } while(0);

        for(auto packet: packets) {
            auto & req = *(Packet *)packet.buf;

            switch(req.type) {
            case KV_OP_META_APPEND:
                processAppend(meta_, packet.buf, packet.cb);
                break;

            case KV_OP_META_GET:
                processGet(meta_, packet.buf, packet.cb);
                break;

            case KV_OP_DATA_APPEND:
                processAppend(data_, packet.buf, packet.cb);
                break;

            case KV_OP_DATA_GET:
                processGet(data_, packet.buf, packet.cb);
                break;

            case KV_OP_CLEAR:
                //TODO: clear local data
                break;

            default:
                LOG(ERROR) << "unknown rpc type: " << req.type;
                packet.cb(nullptr, 0);
                break;
            }
            delete [] packet.buf;
        }
    }
}


void RpcProcess::processAppend(DataMgr & target, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *) buf;
    int key_size = *(int *)req.buf;
    int val_size = *(int *)(req.buf + sizeof(int));
    if (key_size + val_size + sizeof(int) * 2 > req.len) {
        KV_LOG(ERROR) << "kv size error: " << key_size << "+" << val_size << "-" << req.len;
        return;
    }

    KVString key;
    KVString val;
    char * key_buf = new char [key_size];
    char * val_buf = new char [val_size];
    memcpy(key_buf, req.buf + sizeof(int) * 2, key_size);
    memcpy(val_buf, req.buf + sizeof(int) * 2 + key_size, val_size);
    key.Reset(key_buf, key_size);
    val.Reset(val_buf, val_size);
    auto pos = target.Append(key, val);

    int    ret_len = PACKET_HEADER_SIZE + sizeof(pos);
    char * ret_buf = new char [ret_len];
    auto & reply = *(Packet *)ret_buf;
    memcpy(reply.buf, (char *)&pos, sizeof(pos));
    reply.len   = sizeof(pos);
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();

    cb(ret_buf, ret_len);

    delete [] ret_buf;
}

void RpcProcess::processGet(DataMgr & target, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;
    if (req.len < sizeof(int64_t) ) {
        KV_LOG(ERROR) << "index size error: " << req.len;
        return;
    }

    auto idx = *(uint64_t *)req.buf;
    KVString key;
    KVString val;
    target.Get(idx, key, val);

    int key_size = key.Size();
    int val_size = val.Size();
    int    ret_len = PACKET_HEADER_SIZE + sizeof(int) * 2  + key_size + val_size;
    char * ret_buf = new char[ret_len];
    auto & reply = *(Packet *)ret_buf;
    memcpy(reply.buf, (char *)&key_size, sizeof(int));
    memcpy(reply.buf + sizeof(int), (char *)&val_size, sizeof(int));
    memcpy(reply.buf + sizeof(int) * 2, key.Buf(), key_size);
    memcpy(reply.buf + sizeof(int) * 2 + key_size, val.Buf(), val_size);
    reply.len   = sizeof(int) * 2 + key_size + val_size;
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();

    cb(ret_buf, ret_len);

    delete [] ret_buf;
}
