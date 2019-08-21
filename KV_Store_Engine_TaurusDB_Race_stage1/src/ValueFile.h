/*
    ValueFile.h ValueFile头文件
*/

#ifndef Value_FILE_H_
#define Value_FILE_H_
//#define
# define likely(x) __builtin_expect(!!(x), 1)
# define unlikely(x) __builtin_expect(!!(x), 0)

#include <sys/mman.h>
#include <malloc.h>
#include <set>
#include <map>
#include <mutex>  
#include <thread> 
#include <condition_variable>
#include "kv_string.h"
#include "params.h"

static thread_local std::unique_ptr<char> readBuffer(static_cast<char*> (memalign((size_t) getpagesize(), 4096))); 
using namespace std;

class ValueFile
{
private:
    //文件相关
    //ValueFile文件描述符fd
    int valueFileFd_;
    //文件写入偏移位置,需要恢复
    size_t valueFilePosition_;

    //文件缓存相关
    //mmap文件，数据恢复
    int valmmapFd;
    //ValueFile的写入缓存，16KB
    char* valueCacheBuffer_;
    //ValueFile的写入缓存位置，取值0-4，等于4时落盘，也需要数据恢复
    size_t valueCacheBufferPosition_;
    // mmapFd
    //元数据相关
    //元数据文件描述符fd
    int valmetaFd;
    //MetaFile的缓存，4KB
    char* valueMetaCacheBuffer_;

    //多线程读优化，缓存   
    //线程是否已经创建
    bool isThreadsCreate;
    //线程数组
    std::thread read_threads_[READ_THREAD_NUM];
    //缓存value数据,3*64MB = 3*64*1024*1024B
    char *valueCache_; 
    //被加载进内存的分区id
    int partitionInMemSet_[READ_THREAD_NUM];
    //std::set<int> partitionInMemSet_;
    //分区数据已读个数统计，即命中个数
    int partitionReadCnt[READ_THREAD_NUM];
    //缓存分区的状态，分区是否加载完成
    bool loadFinish[READ_THREAD_NUM];
    //缓存分区状态，分区是否全部get读取完成
    bool readFinish[READ_THREAD_NUM];
    //互斥量
    std::mutex mutex_[READ_THREAD_NUM];
    //同步的条件变量
    std::condition_variable loadCondition_[READ_THREAD_NUM];
    //
    std::condition_variable readCondition_[READ_THREAD_NUM];

    // 每个缓冲区可装载的Value的个数
    int valuePerBuffer;

    //
    int readLen_;

public:
    ValueFile(const char * dir, int id, bool exist);
    ~ValueFile();

    int GetValueFileFd() const {
        return valueFileFd_;
    }

    char* GetValueCacheBuffer() const {
        return valueCacheBuffer_;
    }

    void SetValue(KVString & val);

    ssize_t GetValue(size_t keyPosition, KVString & val, int state);

    ssize_t GetValue(char* buf, size_t offset);
    
    bool isPartitionInMem(int partitionPosition);

    void LoadDataToMem(int partition);

    void LoadDataToMemRand(int partition);

    void CreateReadLoadThread(int state);

    void FlushValueToDisk();

    void RecoverValue();
};
#endif