
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <set>
#include <map>
#include <unordered_map>
#include <sparsepp/spp.h>
#include <thread>
#include <iostream>
#include <sstream>
#include <string>
#include <exception>
#include <chrono>
#include "utils.h"
#include "kv_string.h"
#include "KeyFile.h"
#include "params.h"

std::shared_ptr<spp::sparse_hash_map<int64_t , int32_t>> KeyFile::keyOrderMaparray_[16];
int KeyFile::mapCnt_ = 0;
std::mutex KeyFile::mutex_[16];
std::condition_variable KeyFile::Condition_;

KeyFile::KeyFile(DataAgent* agent, int id, bool exist)
    :keyFilePosition_(0), keyOrderMap_(new spp::sparse_hash_map<int64_t , int32_t>())
{
    // 设置链接的接口
    // 构建索引(在第一个findkey调用)
    // 设置目前索引含有key的量：keyFilePosition_
    agent_ = agent;
    id_ = id;
    this->keyCacheBuffer_ = new char[4000000*8]; //四百万个8byte
    //int keyNum = ConstructKeyOrderMap(0);
    //keyFilePosition_ = keyNum;
    // 下面与预判有关
    this->state_ = 0;
    keyOrderMaparray_[id_] = this->keyOrderMap_;
}

KeyFile::~KeyFile() {
    KV_LOG(INFO) << "~KeyFile id:" << id_;
    mapCnt_ = 0;
    if(keyCacheBuffer_ != nullptr)
        delete []keyCacheBuffer_;
}

// 构建 map<key, order>
int KeyFile::ConstructKeyOrderMap(int begin) {
    //printf("\nstart KeyFile::ConstructKeyOrderMap\n");
    int32_t pos = begin;
    int64_t k = 0;
    int len = 0;
    std::lock_guard<std::mutex> lock(mutex_[id_]);
    if( (len = agent_->GetKeys(KV_OP_META_GET, pos, this->keyCacheBuffer_, 0, pos)) > 0) { // 接收一次,获取 keyFile
        //printf("start build map:%d\n", id_);
        KV_LOG(INFO) << "start build map:" << id_;
        for (; pos < len; ++pos) {
            k = *(int64_t*)(this->keyCacheBuffer_ + pos * sizeof(int64_t));            
            try
            {
                this->keyOrderMap_->insert(pair<int64_t, int32_t>(k, pos));
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << "keyOrderMap_->insert\n";
                delete []keyCacheBuffer_;
                keyCacheBuffer_ = nullptr;
                return -1;
            }            
        }
    } else {
        delete []keyCacheBuffer_;
        keyCacheBuffer_ = nullptr;
        return -1;
    }
    this->keyFilePosition_ = len;//max(len, this->keyFilePosition_);
    //printf("id:%d,build Map success,len: %d, setting agent.valueFilePositon\n",id_, len);
    KV_LOG(INFO) << "id:" << id_ << ",build Map success,len:" << len << "setting agent.valueFilePositon";
    this->agent_->SetValueFilePosition(this->keyFilePosition_);
    delete []keyCacheBuffer_;
    keyCacheBuffer_ = nullptr;
    return pos;
}

int64_t KeyFile::FindKey(int64_t key, int &fileId) {
    //printf("FindKey,id:%d\n", id_);
    if (unlikely(this->keyOrderMap_->empty())) { // 不存在， 构建 Map
        if(this->ConstructKeyOrderMap(0) == -1)
            return -1;
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));// 确保其他线程进入了map构建阶段
        // 将构建好的map放到全局map数组
        //keyOrderMaparray_[id_] = this->keyOrderMap_;
        // 先找本线程id的map，找到则说明是功能测试，单线程
        spp::sparse_hash_map<int64_t, int32_t>::const_iterator iter = keyOrderMaparray_[id_]->find(key);
        if (iter != keyOrderMaparray_[id_]->end() ) {
            fileId = id_;
            return iter->second;
        }
        //printf("for find,id:%d\n", id_);
        spp::sparse_hash_map<int64_t, int32_t>::const_iterator isiter;
        // for循环查找每个map
        for(int i = 0; i < 16; ++i) {
            if(i == id_)
                continue;
            std::lock_guard<std::mutex> lock(mutex_[i]);
            isiter = keyOrderMaparray_[i]->find(key);
            if (isiter != keyOrderMaparray_[i]->end() ) {
                fileId = i;
                return isiter->second;
            }        
        }

    } else if (unlikely(this->keyOrderMap_->size() < (this->keyFilePosition_))) { // Map 数量落后(kill 阶段重新写入数据)， 继续构建 keyOrderMap_
        this->ConstructKeyOrderMap(this->keyOrderMap_->size());
    }

    // 将构建好的map放到全局map数组
    //keyOrderMaparray_[id_] = this->keyOrderMap_;
    // 先找本线程id的map，找到则说明是功能测试，单线程
    spp::sparse_hash_map<int64_t, int32_t>::const_iterator iter = keyOrderMaparray_[id_]->find(key);
    if (iter != keyOrderMaparray_[id_]->end() ) {
        fileId = id_;
        return iter->second;
    }
    //printf("for find,id:%d\n", id_);
    spp::sparse_hash_map<int64_t, int32_t>::const_iterator isiter;
    // for循环查找每个map
    for(int i = 0; i < 16; ++i) {
        if(i == id_)
            continue;
        isiter = keyOrderMaparray_[i]->find(key);
        if (isiter != keyOrderMaparray_[i]->end() ) {
            fileId = i;
            return isiter->second;
        }        
    }
    return -1;
}

int64_t KeyFile::GetKeyPosition(KVString & key, int &fileId) {
    int64_t pos = this->FindKey(*(int64_t*)(key.Buf()), fileId);
//    KV_LOG(INFO) << "cfk:" << *((uint64_t*)key.Buf()) << " pos:" << pos;
    if (this->state_ == 0) { // 0 : 随机读，判断状态
        JudgeStage(pos);
    }
    return pos;
}

int KeyFile::GetState() {
    return this->state_;
}

void KeyFile::SetKey(KVString & key) {
    ++this->keyFilePosition_;
}

void KeyFile::JudgeStage(int pos) {
    if(pos == 0) {
        this->state_ = 1;
        return;
    }
    int key_num = keyFilePosition_;//4000000;keyFilePosition_
    int partition = pos / (PARTITION_SIZE * 1024 / 4);
    if (partition == ((key_num-1) / (PARTITION_SIZE * 1024 / 4))) {
        this->state_ = 2;
        return;
    }
    this->state_ = 0;
}

size_t KeyFile::GetKeyFilePosition(){
    return this->keyFilePosition_;
}
void KeyFile::SetKeyFilePosition(int pos) {
    this->keyFilePosition_ = pos;
}