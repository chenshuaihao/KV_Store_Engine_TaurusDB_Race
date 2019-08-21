
#include <exception>
#include "kv_service.h"
#include "utils.h"

/*
 * 传输类型：
const int KV_OP_META_APPEND = 1;
const int KV_OP_META_GET    = 2; 这里表示 获取 store 端的 KeyFile 文件
const int KV_OP_DATA_APPEND = 3; 这里表示 往 store 端添加 key, val 文件
const int KV_OP_DATA_GET    = 4; 这里表示 根据 pos 向 store 端获取 key, val 文件
const int KV_OP_CLEAR       = 5; 表示 使 store 端删除文件
 *
 * */
bool KVService::Init(const char * host, int id) {
    //  根据url链接客户端
    //  恢复meta数据（key-pos）
    string tmp(host);
    host_ = tmp.substr(6);
    id_ = id;
    std::string p = std::to_string(9527+id);
    KV_LOG(INFO) << "Init: try to connect server:" << host_.c_str();
    data_ = new DataAgent(id);
    data_->Init(host_.c_str(), p.c_str());
    keyFile_ = new KeyFile(data_, id, false);
    return true;
}

void KVService::Close() {
    KV_LOG(INFO) << "KVService::Close(),id:" << id_;
    //data_->Release();
    delete data_;
    delete keyFile_;
}

int KVService::Set(KVString & key, KVString & val) {
    keyFile_->SetKey(key);
    uint64_t pos = data_->Append(KV_OP_DATA_APPEND, key, val);
    return 1;
}

int KVService::Get(KVString & key, KVString & val) {
//    int state = 0;
    int fileId = -1;
    int64_t keyPosition = -1;
    try
    {
        keyPosition = keyFile_->GetKeyPosition(key, fileId); // 获取fileId
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << "GetKeyPosition\n";
        return -1;
    }
    if(keyPosition != -1) {
        std::string port = std::to_string(9527+fileId);
        if(fileId != id_) { // fileId != id_ ， 肯定处于性能测试阶段；可以关掉 set 端口， 建立新的端口
            // 非本线程id，关闭 set 端口
            data_->CloseSetPort();
            // add 单线程 测试：
//            data_->Init(host_.c_str(), port.c_str());
        }
//        printf("kv_service:: before createReadConnectPool\n");
        data_->CreateReadConnectPool(host_.c_str(), port.c_str());
        //从缓存读取value， 每次都会对fileId进行判断， 判断出fileId之后，取缓存
//        printf("kv_service:: GetValueFromCache\n");
        data_->GetValueFromCache(keyPosition, fileId, key, val, keyFile_->GetState());
        //data_->Get(KV_OP_DATA_GET, keyPosition, fileId, key, val, keyFile_->GetState());
    }
    else
    {
        printf("KVService::Get, find key pos is -1\n");
        return -1;
    }
}
