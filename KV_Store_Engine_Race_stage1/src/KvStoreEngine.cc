/*

*/
#include <unistd.h>
#include <fcntl.h>  

#include <sstream>
#include <string>
#include <thread> 

#include "KvStoreEngine.h"
#include "ValueFile.h"
#include "KeyFile.h"
#include "params.h"

/*
// KeyFile* keyFiles[FILE_NUM];
// ValueFile* valueFiles[FILE_NUM];
// BufferFile* bufferFiles[FILE_NUM];
*/
KvStoreEngine::KvStoreEngine(/* args */) {
    threadId_ = -1;
    stagetype_ = ORDER_READ;
    keyFile_ = nullptr;
    valueFile_ = nullptr;
}

KvStoreEngine::~KvStoreEngine() {
    //delete []valueCache_;
}


bool KvStoreEngine::Init(const char * dir, int id) {
    
    string path(dir);
    std::ostringstream ss;
    ss << path << "/value-" << id;
    string filePath = ss.str();

    //判断线程标识id对应的文件是否已经存在
    if(access(filePath.data(), 0) != -1) {
        //如果文件已经存在，说明现在是随机读取阶段
        std::string s(dir);
        dir_ = s;
        threadId_ = id;
        stagetype_ = RANDOM_READ;

        keyFile_ = new KeyFile(dir, id, true);
        valueFile_ = new ValueFile(dir, id, true);
        //读取keyFile文件，构件索引

        //valueFile_->CreateReadLoadThread();
    }
    //第一次init
    else {
        //如果文件不存在，说明该线程第一次调用Init，创建keyFiles、valueFiles、bufferFiles（缓冲区丢失恢复用）
        std::string s(dir);
        dir_ = s;
        threadId_ = id;
        stagetype_ = ORDER_READ;
        keyFile_ = new KeyFile(dir, id, false);
        valueFile_ = new ValueFile(dir, id, false);
        //bufferFile = new bufferFile(dir, id);
    }
}

void KvStoreEngine::Close() {
    delete valueFile_;
    delete keyFile_;
}

int KvStoreEngine::Set(KVString &key, KVString & val) {

    valueFile_->SetValue(val);
    keyFile_->SetKey(key);    
}

int KvStoreEngine::Get(KVString &key, KVString & val) {
    int state = 0;
    int64_t keyPosition = keyFile_->GetKeyPosition(key, state);
    if(keyPosition != -1)
        valueFile_->GetValue(keyPosition, val, state);
    else
        return -1;
    /*
    // if(stage == random) {
    //     //随机读阶段
    //     //1.根据key查找索引，看看文件偏移位置offset，看是否在内存中命中
    //     //2.如果命中读内存位置，拷贝到val，淘汰更新
    //     //3.否，pread对应位置
    // }
    // else {
    //     //顺序读阶段
    //     //这里顺序读，好像也不用找索引也行，搞个计数器？
    //     //1.根据key查找索引，看看位置offset，按道理应该已经提前加载到内存了
    //     //2.读内存位置，拷贝到val
    //     //3.提前加载到内存
    // }
     */
}





