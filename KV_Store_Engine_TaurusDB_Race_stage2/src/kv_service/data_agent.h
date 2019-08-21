#pragma once

#include <sys/mman.h>
#include <malloc.h>
#include <cstdint>
#include <map>
#include <set>
#include <memory>
#include <mutex>  
#include "params.h"
#include "kv_string.h"
#include "tcp_client.h"
#include <thread> 
#include <condition_variable>
#include <unistd.h>
static thread_local std::unique_ptr<char> readBuffer(static_cast<char*> (memalign((size_t) getpagesize(), 4096))); 

using asio::ip::tcp;

class DataAgent {
public:
    DataAgent(int id);
    ~DataAgent();

    int Init(const char * host, const char * port);

    // 新connect到指定服务器，get val用
//    int Connect(const char * url, int fileId);
    // 关闭 set 端口
    int CloseSetPort();

    // 创建连接池
    void CreateReadConnectPool(const char * host, const char * port);
    // 关闭连接池
    void CloseReadConnectPool();

    void Release();

    void Clear();

    uint64_t Append(uint32_t type, KVString & key, KVString & val);

    void CreateReadLoadThread(int state, int fileId);

    void LoadDataToMem(int partition, int fileId);

    void LoadDataToMemRand(int partition, int fileId);

    bool isPartitionInMem(int partitionPosition);

    int GetValueFromCache(uint64_t pos, int fileId, KVString & key, KVString &val, int state);

    int Get(uint32_t type, uint64_t pos, TcpClient * tcpClient, KVString &key, KVString &val, int state);

    int GetKeys(uint32_t type, uint64_t pos, char* key_buf, int state, int  cur_key_num);

    void SetValueFilePosition(size_t pos);

    int GetValueFilePosition();
protected:
    // dataAgent连接的store服务器

    // 本线程id
    int id_;
    // fd_, 与本线程id直接对应起来的，与fdarray_[id_]相等
//    tcp::socket fd_ = -1;
    TcpClient * fd_;
    // fd数组，存储连接了store服务器的fd
    // 全局valueCache_
    static std::shared_ptr<char> valueCache_[16];
    std::shared_ptr<char> valueCachelocal_;
    // 加载完缓冲区没有
    bool isThreadsCreate;
    // read线程
    std::thread read_threads_[READ_THREAD_NUM];
    // 分区数据已读个数统计，即命中个数
    int partitionInMemSet_[READ_THREAD_NUM];
    //互斥量
    std::mutex mutex_[READ_THREAD_NUM];
    //缓存分区的状态，分区是否加载完成
    bool loadFinish[READ_THREAD_NUM];
    //缓存分区状态，分区是否全部get读取完成
    bool readFinish[READ_THREAD_NUM];
    //同步的条件变量
    std::condition_variable loadCondition_[READ_THREAD_NUM];
    std::condition_variable readCondition_[READ_THREAD_NUM];
    //computing point is end once
    int end_[READ_THREAD_NUM];
    // 倒序加载时的计数器
    int invertedOrderCnt_[READ_THREAD_NUM];
    // 接收数据的连接池：readConnectPool_[0] 用于处理 disk read,
//    tcp::socket readConnectPool_[READ_THREAD_NUM + 1];
    TcpClient * readConnectPool_[READ_THREAD_NUM + 1];
    // 每个缓冲区可装载的Value的个数
    int valuePerBuffer;
    size_t valueFilePosition_;
};
