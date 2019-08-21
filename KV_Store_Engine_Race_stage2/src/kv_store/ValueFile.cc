////////////////////////////////////////////////////////
/*
    异步set版本，双缓冲区，一个存储IO线程负责在后台pwrite
*/
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <set>
#include <map>
#include <thread>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#include "../include/utils.h"
#include "../third_party/memcopy4k.h"
#include "ValueFile.h"
#include "FastMemcpy.h"



ValueFile::ValueFile(const char * dir, int id, bool exist)
    : valueFileFd_(-1), valueFilePosition_(0), valmmapFd(-1), valueCacheBuffer_(nullptr), 
    valmetaFd(-1), valueMetaCacheBuffer_(nullptr), isThreadsCreate(false), valueCache_(nullptr),
    isWriteThreadCreate(false), curWriteBufferIndex_(0), curReadBufferIndex_(0)
{
    // 每个缓冲区可装载的 value 个数
    this->valuePerBuffer = PARTITION_SIZE << 8;
    this->readLen_ = VALUE_NUM_PER_READ * 4096;
    for (int j = 0; j < WRITE_BUFFER_NUM; ++j) {
        this->valueCacheBufferPosition_[j] = 0;
        this->bufferMarkup_[j] = false;
    }

    for(int i = 0; i < READ_THREAD_NUM; ++i) {
        partitionInMemSet_[i] = -1;
        partitionReadCnt[i] = 0;
        loadFinish[i] = false;
        readFinish[i] = true;
        end_[i] = false;
        invertedOrderCnt_[i] = 0;
    }
    std::ostringstream fd;
    fd << dir << "/value-" << id;
    //string filePath = fd.str();
    this->valueFileFd_ = open(fd.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
    //fallocate(this->valueFileFd_, 0, 0, SIZE_PER_VALUE_FILE);
    //ftruncate(this->valmmapFd, 245*64*1024*1024);  

    //开辟内存空间放value
    valueCache_ = static_cast<char*> (memalign((size_t) getpagesize(), READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024));
    
    if(!exist) {
        //valmmap
        std::ostringstream valmmap;
        valmmap << dir << "/valmmap-" << id;
        this->valmmapFd = open(valmmap.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
        ftruncate(this->valmmapFd, WRITE_BUFFER_NUM * VALUE_BLOCK_SIZE);        
/*
        //选择1：开mmap,ValueFile的写入缓存
        //这里只开了一个value缓冲区，我觉得应该要开多个的，这样才能不断堆数据进去
        //想法是搞个value缓冲区池管理，似乎更好
*/
        this->valueCacheBuffer_ = static_cast<char*>(mmap(nullptr, WRITE_BUFFER_NUM * VALUE_BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmmapFd, 0));
        memset(this->valueCacheBuffer_, 0, WRITE_BUFFER_NUM * VALUE_BLOCK_SIZE);
        /*
        //选择2：对应下面选择2，使用value缓冲区池管理 pool
        //pvalueCacheBufferPool = new ValueCacheBufferPool(size);
        //this->valueCacheBuffer_ = pvalueCacheBufferPool->getFreeValueCacheBuffer();  
*/
        //valmeta
        std::ostringstream valmeta;
        valmeta << dir << "/valmetafd-" << id;
        this->valmetaFd = open(valmeta.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
        ftruncate(this->valmetaFd, 4096);     
        this->valueMetaCacheBuffer_ = static_cast<char*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmetaFd, 0));
        memset(this->valueCacheBuffer_, 0, 4096);
        *((size_t*)(this->valueMetaCacheBuffer_)) = 0;
    }
    else {
        //value数据恢复
        std::ostringstream valmmap;
        valmmap << dir << "/valmmap-" << id;
        this->valmmapFd = open(valmmap.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
        this->valueCacheBuffer_ = static_cast<char*>(mmap(nullptr, WRITE_BUFFER_NUM * VALUE_BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmmapFd, 0));

        //valueMeta数据恢复
        std::ostringstream valmeta;
        valmeta << dir << "/valmetafd-" << id;
        this->valmetaFd = open(valmeta.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777); 
        this->valueMetaCacheBuffer_ = static_cast<char*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmetaFd, 0));

        // valueCacheBufferPosition_ = *((size_t*)(this->valueMetaCacheBuffer_));
        // valueFilePosition_ = *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(size_t)));
        // //kill 数据恢复落盘
        // FlushValueToDisk();
                
        //asyn set 恢复两个set缓存分区的value个数
        for (int i = 0; i < WRITE_BUFFER_NUM; ++i) {
            this->valueCacheBufferPosition_[i] = *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + i * sizeof(size_t)));
        }
        valueFilePosition_ = *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + WRITE_BUFFER_NUM * sizeof(size_t)));

        //printf("curReadBufferIndex_:%d\n", *((int*)(this->valueMetaCacheBuffer_)));
        //printf("v1:%ld, v2:%ld\n", valueCacheBufferPosition_[0], valueCacheBufferPosition_[1]);
        //printf("valueFilePosition_:%ld\n", valueFilePosition_);
        //kill 数据恢复落盘
        //FlushValueToDisk();
        FlushValueToDiskRecover();
    }
}

ValueFile::~ValueFile() { 
    if(isThreadsCreate) {
        for(auto &t : read_threads_)
            t.detach();
    }
    if(this->isWriteThreadCreate) {
        this->isWriteThreadCreate = false; 
        this->bufferMarkup_[this->curReadBufferIndex_] = true;      
        this->write_thread_.detach();
    }
    //close 数据落盘
    // FlushValueToDisk();
    FlushValueToDiskRecover();
    if(valueCache_ != nullptr)
        free(valueCache_);

    munmap(valueCacheBuffer_, WRITE_BUFFER_NUM * VALUE_BLOCK_SIZE);
    munmap(valueMetaCacheBuffer_, 4096);
    close(this->valmetaFd);
    close(this->valmmapFd);
    close(this->valueFileFd_);
}

void ValueFile::SetValue(KVString & val) {
    //当前缓冲区为 valueCacheBuffer_, 缓冲区待写入位置为 valueCacheBufferPosition_    
    //写value进16KB缓冲区，位置递增
    //memcpy(valueCacheBuffer_ + (valueCacheBufferPosition_ << 12), val.Buf(), 4096);
    // TODO 若没创建 io 线程，则创建
    if (unlikely(!this->isWriteThreadCreate)) {
        this->isWriteThreadCreate = true; // 先设为 true； WriteDataToDisk才能正常工作
        this->write_thread_ = std::thread(&ValueFile::WriteDataToDisk, this);
    }
    // TODO 按顺序写入， 按顺序读取
    std::unique_lock<std::mutex> lock(mutex1_[this->curWriteBufferIndex_]);
    while (this->bufferMarkup_[this->curWriteBufferIndex_]) {//当前缓冲区可读（没落盘），线程等待
        //printf("waiting : this.curWriteBufferIndex:: %d \n", this->curWriteBufferIndex_);
        loadCondition1_[this->curWriteBufferIndex_].wait(lock);
    }
    //  TODO 对缓冲区上锁，写入 ------- 现在是直接写入，没有上锁
    memcpy_4k(valueCacheBuffer_ + this->curWriteBufferIndex_ * VALUE_BLOCK_SIZE + (valueCacheBufferPosition_[this->curWriteBufferIndex_] << 12), val.Buf());
    valueCacheBufferPosition_[this->curWriteBufferIndex_]++;
    //数据恢复记录
    *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + this->curWriteBufferIndex_ * sizeof(size_t))) = valueCacheBufferPosition_[this->curWriteBufferIndex_];
    // TODO 当写入次数满 xx 个时，将标记区设为 true, 并将当前写入的缓冲区下标自增
    if (this->valueCacheBufferPosition_[this->curWriteBufferIndex_] == BUFFER_VALUE_NUM) {
        this->bufferMarkup_[this->curWriteBufferIndex_] = true;
        readCondition1_[this->curWriteBufferIndex_].notify_one();
        this->curWriteBufferIndex_ ++;
        if (this->curWriteBufferIndex_ == WRITE_BUFFER_NUM) {
            this->curWriteBufferIndex_ = 0;
        }
        
    }
}
void ValueFile::WriteDataToDisk() {
    // TODO 循环条件： isWriteThreadCreate = true
    while(this->isWriteThreadCreate) {
        // TODO 等待锁， 获取可读的缓冲区的 valueCacheBufferIndex
        std::unique_lock<std::mutex> lock(mutex1_[this->curReadBufferIndex_]);
        while (!this->bufferMarkup_[this->curReadBufferIndex_]) { //当前缓冲区不可读，只能等待
            //printf("waiting : this.curReadBufferIndex:: %d \n", this->curReadBufferIndex_);
            readCondition1_[this->curReadBufferIndex_].wait(lock);
        }
        if(!this->isWriteThreadCreate)
            break;
        // TODO 调用FlushValueToDisk(int valueCacheBufferIndex)， 将缓冲区的数据写入
        *((int*)(this->valueMetaCacheBuffer_)) = this->curReadBufferIndex_;
        FlushValueToDisk(this->curReadBufferIndex_);
        this->bufferMarkup_[this->curReadBufferIndex_] = false;
        loadCondition1_[this->curReadBufferIndex_].notify_one();
        this->curReadBufferIndex_++;
        if (this->curReadBufferIndex_ == WRITE_BUFFER_NUM) {
            this->curReadBufferIndex_ = 0;
        }        
    }
}
void ValueFile::FlushValueToDisk(int valueCacheBufferIndex) {
    //printf("flush\n");
    *((int*)(this->valueMetaCacheBuffer_)) = valueCacheBufferIndex; //记录被kill时候，正在pwrite的valueCacheBufferIndex
    pwrite(valueFileFd_, valueCacheBuffer_ + valueCacheBufferIndex * VALUE_BLOCK_SIZE, valueCacheBufferPosition_[valueCacheBufferIndex] * VALUE_SIZE, valueFilePosition_);
    // TODO valueFilePosition_ 在使用多个后台 dio线程的情况下，需要同步，本程序只用一个后台线程，则不需要同步
    valueFilePosition_ += valueCacheBufferPosition_[valueCacheBufferIndex] * VALUE_SIZE;
    //valueFilePositionCache_[valueCacheBufferIndex] += valueCacheBufferPosition_[valueCacheBufferIndex] * VALUE_SIZE;
    valueCacheBufferPosition_[valueCacheBufferIndex] = 0;    
    *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + WRITE_BUFFER_NUM * sizeof(size_t))) = valueFilePosition_;
    *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + valueCacheBufferIndex * sizeof(size_t))) = valueCacheBufferPosition_[valueCacheBufferIndex];    
    //*((size_t*)(this->valueMetaCacheBuffer_ + WRITE_BUFFER_NUM * sizeof(size_t) + sizeof(size_t) + valueCacheBufferIndex * sizeof(size_t))) = valueFilePositionCache_[valueCacheBufferIndex];
    memset(valueCacheBuffer_ + valueCacheBufferIndex * VALUE_BLOCK_SIZE, 0, VALUE_BLOCK_SIZE);
}
void ValueFile::FlushValueToDiskRecover() {
    //printf("FlushValueToDiskRecover\n");    
    int valueCacheBufferIndex = *((int*)(this->valueMetaCacheBuffer_));
    for (int i = 0; i < WRITE_BUFFER_NUM; i++) {
        int j = (valueCacheBufferIndex + i) % WRITE_BUFFER_NUM;
        if (valueCacheBufferPosition_[j] != 0) {
            printf("Flush Num:%ld\n", valueCacheBufferPosition_[j]);
            pwrite(valueFileFd_, valueCacheBuffer_ + j * VALUE_BLOCK_SIZE, valueCacheBufferPosition_[j] * VALUE_SIZE, valueFilePosition_);
            valueFilePosition_ += valueCacheBufferPosition_[j] * VALUE_SIZE;
            valueCacheBufferPosition_[j] = 0;
            *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + j * sizeof(size_t))) = valueCacheBufferPosition_[j];
        }
    }
    *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(int) + WRITE_BUFFER_NUM * sizeof(size_t))) = valueFilePosition_;
    memset(valueCacheBuffer_, 0, WRITE_BUFFER_NUM * VALUE_BLOCK_SIZE);
    //memset(valueMetaCacheBuffer_, 0, 4096);
}
//消费者
ssize_t ValueFile::GetValue(size_t keyPosition, KVString & val, int state) {
    
    FlushValueToDiskRecover();
   if(state == 1) {
        if(unlikely(!isThreadsCreate)) {
            CreateReadLoadThread(state);
        }
        //计算key所在的文件分区位置
        int partitionPosition = keyPosition / this->valuePerBuffer;
        //计算在第几个缓存分区
        int partition = partitionPosition % READ_THREAD_NUM;
        //printf("keyPosition: %d, partitionPosition: %d, partition: %d\n", keyPosition, partitionPosition, partition);
        //printf("partitionInMemSet_[0]: %d, partitionInMemSet_[1]: %d, partitionInMemSet_[2]: %d, partitionInMemSet_[3]: %d\n", partitionInMemSet_[0], partitionInMemSet_[1], partitionInMemSet_[2], partitionInMemSet_[3]);
        //判断key所在的分区是否被加载到内存中
        if(isPartitionInMem(partitionPosition) && end_[partition] == false) {
            //printf("mem read\n");
            std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况
            while(!loadFinish[partition])//loadFinish[partition] == false
            {
                readCondition_[partition].wait(lock);
            }
            readFinish[partition] = false;
            if(val.Size() == 4096) {
                KVString tmp(std::move(val));
                const char * str = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096;
                memcpy_4k(const_cast<char*> (tmp.Buf()), str);
                val = std::move(tmp);          
            } else {
                //printf("********KVString tmp KVString tmp KVString tmp KVString tmp KVString tmp********\n");
                KVString tmp(valueCache_ + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096, 4096);
                val = std::move(tmp);  
            }

            if((keyPosition + 1) % this->valuePerBuffer == 0 || ((keyPosition+1) * 4096) >= valueFilePosition_) {
                //已经get读完本分区数据了
                partitionInMemSet_[partition] = -1;
                readFinish[partition] = true;
                loadFinish[partition] == false;
                loadCondition_[partition].notify_one();//唤醒等待的分区IO线程            
            }
            return 4096;
        } else {
            //printf("disk read\n");
            auto buffer = readBuffer.get();
            ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
            KVString tmp(buffer, 4096);
            val = std::move(tmp);
            return ret;         
        }
   } else if(state == 2) {
        //static int mem_readcnt = 0;
        //static int disk_readcnt = 0;
        if(unlikely(!isThreadsCreate)) {
            CreateReadLoadThread(state);
        }
        int lastpartion = (valueFilePosition_ / 4096 - 1) / this->valuePerBuffer; // (1025-1)/1024, (1024-1)/1024
        int last_partion_num = (valueFilePosition_ / 4096) % this->valuePerBuffer;
        //计算key所在的文件分区位置
        int partitionPosition = keyPosition / this->valuePerBuffer;
        //计算在第几个缓存分区
        int partition = partitionPosition % READ_THREAD_NUM;
        //printf("keyPosition: %d, partitionPosition: %d, partition: %d\n", keyPosition, partitionPosition, partition);
        //printf("partitionInMemSet_[0]: %d, partitionInMemSet_[1]: %d, partitionInMemSet_[2]: %d, partitionInMemSet_[3]: %d\n", partitionInMemSet_[0], partitionInMemSet_[1], partitionInMemSet_[2], partitionInMemSet_[3]);
        //判断key所在的分区是否被加载到内存中
        if(isPartitionInMem(partitionPosition) && end_[partition] == false) {
            //printf("mem read:%d\n", ++mem_readcnt);
            std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况
            while(!loadFinish[partition])//loadFinish[partition] == false
            {
                readCondition_[partition].wait(lock);
            }
            readFinish[partition] = false;
            KVString tmp(valueCache_ + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096, 4096);
            val = std::move(tmp);

            if((keyPosition + 1) % this->valuePerBuffer == 0 || ((keyPosition+1) * 4096) >= valueFilePosition_) {
                //已经get读完本分区数据了
                partitionInMemSet_[partition] = -1;
                readFinish[partition] = true;
                loadFinish[partition] == false;
                loadCondition_[partition].notify_one();//唤醒等待的分区IO线程
            }
            return 4096;
        } else {
            //printf("disk read:%d\n", ++disk_readcnt);
            auto buffer = readBuffer.get();
            ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
            KVString tmp(buffer, 4096);
            val = std::move(tmp);
            return ret;         
        }
   } else {
        auto buffer = readBuffer.get();
        ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
        KVString tmp(buffer, 4096);
        val = std::move(tmp);
        return ret;   
   }

}

//判断分区在不在内存
bool ValueFile::isPartitionInMem(int partitionPosition) {
    for(int i = 0; i < READ_THREAD_NUM; ++i) {
        if(partitionPosition == partitionInMemSet_[i])
            return true;
    }
    return false;
}

//多线程读用到的函数
ssize_t ValueFile::GetValue(char* buf, size_t offset) {
    
    
    ssize_t ret = pread(this->valueFileFd_, buf, 16384, offset);
    
    return ret;
}

//生产者，partition表示第几分区，0,1,2
void ValueFile::LoadDataToMem(int partition) {

    char* cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024;
    size_t len = PARTITION_SIZE * 1024 * 1024; //分区大小
    size_t offset = PARTITION_SIZE * 1024 * 1024 * partition; //对应的文件偏移位置
    int curPartition = partition;
    bool loop = true;
    int cnt = partition;
    partitionInMemSet_[partition] = partition;

    while(loop) {
//    {
        //判断看看本区域是否可写，是的话，加锁加载硬盘数据                 
        std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况        
        while(!readFinish[partition])
        {
            loadCondition_[partition].wait(lock);
        }

        partitionInMemSet_[partition] = curPartition;//当前加载的文件分区，这一句放哪里好呢？
        loadFinish[partition] = false;
        for(int i = 0; i * 16384 < len; ++i) {      
            if(unlikely(offset >= valueFilePosition_)) {
//                //IO线程读取文件偏移超出实际长度，结束线程
                //printf("------------ load thread:%d, IO线程读取文件偏移超出实际长度，结束线程!!!-----------\n", partition);
                loop = false;
		        end_[partition] = true;
                break;
            }
            GetValue(cache, offset);
            cache += 16384;
            offset += 16384;  
        }
        //printf("------------ load thread: %d, file partition: %d load finish!!!-----------\n", partition, cnt);
        //printf("------------ load thread: %d, 2 readFinish[partition]: %d!!!-----------\n", partition, readFinish[partition]);

        cnt += READ_THREAD_NUM;

        cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024; //本区数据已经全部加载，重新覆盖缓存区

        offset += len * (READ_THREAD_NUM - 1);  //文件偏移到下一个分区，+2

        curPartition += READ_THREAD_NUM;//下一个待读文件分区

        loadFinish[partition] = true;
        readFinish[partition] = false;
//    }
        readCondition_[partition].notify_one();//唤醒等待的测试线程
    }        
}

void ValueFile::LoadDataToMemRand(int partition) {

    char* cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024;
    long long int len = PARTITION_SIZE * 1024 * 1024; //分区大小
    long long int offset = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024))\
     * (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024) + PARTITION_SIZE * 1024 * 1024 * partition; //对应的文件偏移位置
    int curPartition = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024)) * READ_THREAD_NUM + partition;
    bool loop = true;
    int cnt = partition;

    //如果当前线程初始偏移超出文件长度的话，执行校正，往前偏移n个分区单位，n为线程数，例如：向前偏移3*64MB
    if(offset >= valueFilePosition_) {
        offset -= READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024;
        curPartition -= READ_THREAD_NUM;
    }
    partitionInMemSet_[partition] = partition;

    while(loop) {
    {
        //判断看看本区域是否可写，是的话，加锁加载硬盘数据                 
        std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况        
        while(!readFinish[partition])
        {
            loadCondition_[partition].wait(lock);
        }

        partitionInMemSet_[partition] = curPartition;//当前加载的文件分区，这一句放哪里好呢？        
        loadFinish[partition] = false;
        size_t ret = 0;
        for(int i = 0; i * 16384 < len; ++i) {      
            if(unlikely(offset < 0)) {
                //IO线程读取文件偏移超出实际长度，结束线程
                //printf("------------ load thread:%d, IO线程读取文件偏移超出实际长度，结束线程!!!-----------\n", partition);
                loop = false;
                end_[partition] = true;
                break;
            }
            GetValue(cache, offset);
            //if(ret == 0) break;
            cache += 16384;
            offset += 16384;  
        }
        //printf("------------ load thread: %d, file partition: %d load finish!!!-----------\n", partition, cnt);
        //printf("------------ load thread: %d, 2 readFinish[partition]: %d!!!-----------\n", partition, readFinish[partition]);

        cnt += READ_THREAD_NUM;

        cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024; //本区数据已经全部加载，重新覆盖缓存区

        offset -= len * (READ_THREAD_NUM + 1);  //文件偏移到下一个分区，+2

        curPartition -= READ_THREAD_NUM;//下一个待读文件分区

        loadFinish[partition] = true;
        readFinish[partition] = false;
        invertedOrderCnt_[partition] = 0;
    }
        readCondition_[partition].notify_one();//唤醒等待的测试线程
    }        
}

void ValueFile::CreateReadLoadThread(int state) {
    //创建DIO线程，预读，以及在后台加载文件到内存
    //std::thread read_threads[READ_THREAD_NUM];
    KV_LOG(INFO) << "ValueFile::CreateReadLoadThread";
    if(state == 1) {
        for(int i = 0; i < READ_THREAD_NUM; ++i) { 
            //partitionInMemSet_[i] = i;
            read_threads_[i] = std::thread(&ValueFile::LoadDataToMem, this, i);
        }
    } else {
        for(int i = 0; i < READ_THREAD_NUM; ++i) { 
            //int cur = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024)) * READ_THREAD_NUM + partition;   
            //partitionInMemSet_[i] = i;
            read_threads_[i] = std::thread(&ValueFile::LoadDataToMemRand, this, i);
        }
    }

    isThreadsCreate = true;
}

void ValueFile::RecoverValue() {

}


////////////////////////////////////////////////////////
/*
    同步set版本，来n个value，阻塞式pwrite一次，
*/

// #include <stdio.h>
// #include <fcntl.h>
// #include <sys/mman.h>
// #include <unistd.h>
// #include <sys/types.h>
// #include <set>
// #include <map>
// #include <thread>
// #include <iostream>
// #include <sstream>
// #include <string>
// #include <utility>

// #include "../include/utils.h"
// #include "../third_party/memcopy4k.h"
// #include "ValueFile.h"
// #include "FastMemcpy.h"



// ValueFile::ValueFile(const char * dir, int id, bool exist)
//     : valueFileFd_(-1), valueFilePosition_(0), valmmapFd(-1), valueCacheBuffer_(nullptr), valueCacheBufferPosition_(0), 
//     valmetaFd(-1), valueMetaCacheBuffer_(nullptr), isThreadsCreate(false), valueCache_(nullptr)
// {
//     // 每个缓冲区可装载的 value 个数
//     this->valuePerBuffer = PARTITION_SIZE * 1024 / 4;

//     for(int i = 0; i < READ_THREAD_NUM; ++i) {
//         partitionInMemSet_[i] = -1;
//         partitionReadCnt[i] = 0;
//         loadFinish[i] = false;
//         readFinish[i] = true;
//         end_[i] = false;
//         invertedOrderCnt_[i] = 0;
//     }
//     std::ostringstream fd;
//     fd << dir << "/value-" << id;
//     //string filePath = fd.str();
//     this->valueFileFd_ = open(fd.str().data(), O_CREAT | O_RDWR | O_DIRECT | O_NOATIME, 0777);
//     //fallocate(this->valueFileFd_, 0, 0, SIZE_PER_VALUE_FILE);
//     //ftruncate(this->valmmapFd, 245*64*1024*1024);  

//     //开辟内存空间放value
//     valueCache_ = static_cast<char*> (memalign((size_t) getpagesize(), READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024));
    
//     if(!exist) {
//         //valmmap
//         std::ostringstream valmmap;
//         valmmap << dir << "/valmmap-" << id;
//         this->valmmapFd = open(valmmap.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
//         ftruncate(this->valmmapFd, VALUE_BLOCK_SIZE);        
// /*
//         //选择1：开mmap,ValueFile的写入缓存
//         //这里只开了一个value缓冲区，我觉得应该要开多个的，这样才能不断堆数据进去
//         //想法是搞个value缓冲区池管理，似乎更好
// */
//         this->valueCacheBuffer_ = static_cast<char*>(mmap(nullptr, VALUE_BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmmapFd, 0));
//         memset(this->valueCacheBuffer_, 0, VALUE_BLOCK_SIZE);
//         /*
//         //选择2：对应下面选择2，使用value缓冲区池管理 pool
//         //pvalueCacheBufferPool = new ValueCacheBufferPool(size);
//         //this->valueCacheBuffer_ = pvalueCacheBufferPool->getFreeValueCacheBuffer();  
// */
//         //valmeta
//         std::ostringstream valmeta;
//         valmeta << dir << "/valmetafd-" << id;
//         this->valmetaFd = open(valmeta.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
//         ftruncate(this->valmetaFd, 4096);     
//         this->valueMetaCacheBuffer_ = static_cast<char*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmetaFd, 0));
//         *((size_t*)(this->valueMetaCacheBuffer_)) = 0;
//     }
//     else {
//         //value数据恢复
//         std::ostringstream valmmap;
//         valmmap << dir << "/valmmap-" << id;
//         this->valmmapFd = open(valmmap.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
//         this->valueCacheBuffer_ = static_cast<char*>(mmap(nullptr, VALUE_BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmmapFd, 0));

//         //valueMeta数据恢复
//         std::ostringstream valmeta;
//         valmeta << dir << "/valmetafd-" << id;
//         this->valmetaFd = open(valmeta.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777); 
//         this->valueMetaCacheBuffer_ = static_cast<char*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, this->valmetaFd, 0));
//         valueCacheBufferPosition_ = *((size_t*)(this->valueMetaCacheBuffer_));
//         valueFilePosition_ = *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(size_t)));
//         //kill 数据恢复落盘
//         FlushValueToDisk();
//     }
// }

// ValueFile::~ValueFile() { 
//     if(isThreadsCreate) {
//         for(auto &t : read_threads_)
//             t.detach();
//     }
//     //close 数据落盘
//     FlushValueToDisk();
//     if(valueCache_ != nullptr)
//         free(valueCache_);

//     munmap(valueCacheBuffer_, VALUE_BLOCK_SIZE);
//     munmap(valueMetaCacheBuffer_, 4096);
//     close(this->valmetaFd);
//     close(this->valmmapFd);
//     close(this->valueFileFd_);
// }

// void ValueFile::SetValue(KVString & val) {
//     //当前缓冲区为 valueCacheBuffer_, 缓冲区待写入位置为 valueCacheBufferPosition_    
//     //写value进16KB缓冲区，位置递增
//     //memcpy(valueCacheBuffer_ + (valueCacheBufferPosition_ << 12), val.Buf(), 4096);
//     memcpy_4k(valueCacheBuffer_ + (valueCacheBufferPosition_ << 12), val.Buf());
//     //memcpy_fast(valueCacheBuffer_ + (valueCacheBufferPosition_ << 12), val.Buf(), 4096);
//     valueCacheBufferPosition_++;
//     //数据恢复记录
//     *((size_t*)(this->valueMetaCacheBuffer_)) = valueCacheBufferPosition_;
//     //value缓冲区写满就落盘，1.测试线程亲自落盘，2.或者送入任务队列，让DIO线程落盘。两者二选一，个人认为倾向于后者好一些
//     if (valueCacheBufferPosition_ == BUFFER_VALUE_NUM) {
//         //选择1：测试线程亲自落盘，阻塞
//         pwrite(valueFileFd_, valueCacheBuffer_, VALUE_BLOCK_SIZE, valueFilePosition_);
// /*
//         // //选择2：或者送入任务队列，DIO线程落盘，正在落盘的数据可能会丢失（kill），要把元数据记录（id, filePosition）
//         // Task task(valueFileFd_, valueCacheBuffer_, VALUE_BLOCK_SIZE, valueFilePosition_);
//         // dioTaskQueue.push(task);//队列任务太多时，也会阻塞等待，否则测试线程生产太快，DIO线程赶不上，内存一定会爆掉
//         // //从valueCacheBufferPool中取一个空的valueCacheBuffer缓冲区，作为下一次写入用
//         // valueCacheBuffer_ = valueCacheBufferPool->getFreeValueCacheBuffer();
// */
//         valueFilePosition_ += VALUE_BLOCK_SIZE;
//         valueCacheBufferPosition_ = 0;
//         *((size_t*)(this->valueMetaCacheBuffer_)) = valueCacheBufferPosition_;
//         *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(size_t))) = valueFilePosition_;
//         //要不要清空valueCacheBuffer_呢？应该要吧，同步落盘（选择1）应该要，异步落盘（选择2）时交给DIO线程memset
//         //memset(valueCacheBuffer_, 0, VALUE_BLOCK_SIZE);
//     }

//     // if(valueFilePosition_ >= (3980000 << 12) && !isThreadsCreate) {//3944704 55296
//     //     CreateReadLoadThread(1);
//     // }
// }

// //消费者
// ssize_t ValueFile::GetValue(size_t keyPosition, KVString & val, int state) {
    
//     if(valueCacheBufferPosition_ != 0) {
//         //Get 数据落盘
//         FlushValueToDisk();
//     }
//     /*
//     auto buffer = readBuffer.get();
//     ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
//     KVString tmp(buffer, 4096);
//     val = std::move(tmp);
//     return ret;
//     */
//    if(state == 1) {
//         if(unlikely(!isThreadsCreate)) {
//             CreateReadLoadThread(state);
//         }
//         //计算key所在的文件分区位置
//         int partitionPosition = keyPosition / this->valuePerBuffer;
//         //计算在第几个缓存分区
//         int partition = partitionPosition % READ_THREAD_NUM;
//         //printf("keyPosition: %d, partitionPosition: %d, partition: %d\n", keyPosition, partitionPosition, partition);
//         //printf("partitionInMemSet_[0]: %d, partitionInMemSet_[1]: %d, partitionInMemSet_[2]: %d, partitionInMemSet_[3]: %d\n", partitionInMemSet_[0], partitionInMemSet_[1], partitionInMemSet_[2], partitionInMemSet_[3]);
//         //判断key所在的分区是否被加载到内存中
//         if(isPartitionInMem(partitionPosition) && end_[partition] == false) {
//             //printf("mem read\n");
//             std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况
//             while(!loadFinish[partition])//loadFinish[partition] == false
//             {
//                 readCondition_[partition].wait(lock);
//             }
//             readFinish[partition] = false;
//             if(val.Size() == 4096) {
//                 KVString tmp(std::move(val));
//                 const char * str = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096;
//                 memcpy_4k(const_cast<char*> (tmp.Buf()), str);
//                 val = std::move(tmp);          
//             } else {
//                 //printf("********KVString tmp KVString tmp KVString tmp KVString tmp KVString tmp********\n");
//                 KVString tmp(valueCache_ + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096, 4096);
//                 val = std::move(tmp);  
//             }

//             if((keyPosition + 1) % this->valuePerBuffer == 0 || ((keyPosition+1) * 4096) >= valueFilePosition_) {
//                 //已经get读完本分区数据了
//                 partitionInMemSet_[partition] = -1;
//                 readFinish[partition] = true;
//                 loadFinish[partition] == false;
//                 loadCondition_[partition].notify_one();//唤醒等待的分区IO线程            
//             }
//             return 4096;
//         } else {
//             //printf("disk read\n");
//             auto buffer = readBuffer.get();
//             ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
//             KVString tmp(buffer, 4096);
//             val = std::move(tmp);
//             return ret;         
//         }
//    } else if(state == 2) {
//         //static int mem_readcnt = 0;
//         //static int disk_readcnt = 0;
//         if(unlikely(!isThreadsCreate)) {
//             CreateReadLoadThread(state);
//         }
//         int lastpartion = (valueFilePosition_ / 4096 - 1) / this->valuePerBuffer; // (1025-1)/1024, (1024-1)/1024
//         int last_partion_num = (valueFilePosition_ / 4096) % this->valuePerBuffer;
//         //计算key所在的文件分区位置
//         int partitionPosition = keyPosition / this->valuePerBuffer;
//         //计算在第几个缓存分区
//         int partition = partitionPosition % READ_THREAD_NUM;
//         //printf("keyPosition: %d, partitionPosition: %d, partition: %d\n", keyPosition, partitionPosition, partition);
//         //printf("partitionInMemSet_[0]: %d, partitionInMemSet_[1]: %d, partitionInMemSet_[2]: %d, partitionInMemSet_[3]: %d\n", partitionInMemSet_[0], partitionInMemSet_[1], partitionInMemSet_[2], partitionInMemSet_[3]);
//         //判断key所在的分区是否被加载到内存中
//         if(isPartitionInMem(partitionPosition) && end_[partition] == false) {
//             //printf("mem read:%d\n", ++mem_readcnt);
//             std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况
//             while(!loadFinish[partition])//loadFinish[partition] == false
//             {
//                 readCondition_[partition].wait(lock);
//             }
//             readFinish[partition] = false;
//             KVString tmp(valueCache_ + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096, 4096);
//             val = std::move(tmp);

// //            if((keyPosition) % this->valuePerBuffer == 0 || (keyPosition * 4096) >= valueFilePosition_) {
// //                //已经get读完本分区数据了
// //                readFinish[partition] = true;
// //                loadFinish[partition] == false;
// //                loadCondition_[partition].notify_one();//唤醒等待的分区IO线程
// //            }
//             //invertedOrderCnt_[partition] ++;
//             if((keyPosition + 1) % this->valuePerBuffer == 0 || ((keyPosition+1) * 4096) >= valueFilePosition_) {
//                 //已经get读完本分区数据了
//                 partitionInMemSet_[partition] = -1;
//                 readFinish[partition] = true;
//                 loadFinish[partition] == false;
//                 loadCondition_[partition].notify_one();//唤醒等待的分区IO线程
//             }
//             return 4096;
//         } else {
//             //printf("disk read:%d\n", ++disk_readcnt);
//             auto buffer = readBuffer.get();
//             ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
//             KVString tmp(buffer, 4096);
//             val = std::move(tmp);
//             return ret;         
//         }
//    } else {
//         auto buffer = readBuffer.get();
//         ssize_t ret = pread(this->valueFileFd_, buffer, 4096, keyPosition << 12);
//         KVString tmp(buffer, 4096);
//         val = std::move(tmp);
//         return ret;   
//    }

// }

// //判断分区在不在内存
// bool ValueFile::isPartitionInMem(int partitionPosition) {
//     for(int i = 0; i < READ_THREAD_NUM; ++i) {
//         if(partitionPosition == partitionInMemSet_[i])
//             return true;
//     }
//     return false;
// }

// //多线程读用到的函数
// ssize_t ValueFile::GetValue(char* buf, size_t offset) {
    
//     if(valueCacheBufferPosition_ != 0) {
//         //Get 数据落盘
//         FlushValueToDisk();
//     }
//     ssize_t ret = pread(this->valueFileFd_, buf, 16384, offset);
    
//     return ret;
// }

// //生产者，partition表示第几分区，0,1,2
// void ValueFile::LoadDataToMem(int partition) {

//     char* cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024;
//     size_t len = PARTITION_SIZE * 1024 * 1024; //分区大小
//     size_t offset = PARTITION_SIZE * 1024 * 1024 * partition; //对应的文件偏移位置
//     int curPartition = partition;
//     bool loop = true;
//     int cnt = partition;
//     partitionInMemSet_[partition] = partition;

//     while(loop) {
// //    {
//         //判断看看本区域是否可写，是的话，加锁加载硬盘数据                 
//         std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况        
//         while(!readFinish[partition])
//         {
//             loadCondition_[partition].wait(lock);
//         }

//         partitionInMemSet_[partition] = curPartition;//当前加载的文件分区，这一句放哪里好呢？
//         loadFinish[partition] = false;
//         for(int i = 0; i * 16384 < len; ++i) {      
//             if(unlikely(offset >= valueFilePosition_)) {
// //                //IO线程读取文件偏移超出实际长度，结束线程
//                 //printf("------------ load thread:%d, IO线程读取文件偏移超出实际长度，结束线程!!!-----------\n", partition);
//                 loop = false;
// 		        end_[partition] = true;
//                 break;
//             }
//             GetValue(cache, offset);
//             cache += 16384;
//             offset += 16384;  
//         }
//         //printf("------------ load thread: %d, file partition: %d load finish!!!-----------\n", partition, cnt);
//         //printf("------------ load thread: %d, 2 readFinish[partition]: %d!!!-----------\n", partition, readFinish[partition]);

//         cnt += READ_THREAD_NUM;

//         cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024; //本区数据已经全部加载，重新覆盖缓存区

//         offset += len * (READ_THREAD_NUM - 1);  //文件偏移到下一个分区，+2

//         curPartition += READ_THREAD_NUM;//下一个待读文件分区

//         loadFinish[partition] = true;
//         readFinish[partition] = false;
// //    }
//         readCondition_[partition].notify_one();//唤醒等待的测试线程
//     }        
// }

// void ValueFile::LoadDataToMemRand(int partition) {

//     char* cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024;
//     long long int len = PARTITION_SIZE * 1024 * 1024; //分区大小
//     long long int offset = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024))\
//      * (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024) + PARTITION_SIZE * 1024 * 1024 * partition; //对应的文件偏移位置
//     int curPartition = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024)) * READ_THREAD_NUM + partition;
//     bool loop = true;
//     int cnt = partition;

//     //如果当前线程初始偏移超出文件长度的话，执行校正，往前偏移n个分区单位，n为线程数，例如：向前偏移3*64MB
//     if(offset >= valueFilePosition_) {
//         offset -= READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024;
//         curPartition -= READ_THREAD_NUM;
//     }
//     partitionInMemSet_[partition] = partition;

//     while(loop) {
//     {
//         //判断看看本区域是否可写，是的话，加锁加载硬盘数据                 
//         std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况        
//         while(!readFinish[partition])
//         {
//             loadCondition_[partition].wait(lock);
//         }

//         partitionInMemSet_[partition] = curPartition;//当前加载的文件分区，这一句放哪里好呢？        
//         loadFinish[partition] = false;
//         size_t ret = 0;
//         for(int i = 0; i * 16384 < len; ++i) {      
//             if(unlikely(offset < 0)) {
//                 //IO线程读取文件偏移超出实际长度，结束线程
//                 //printf("------------ load thread:%d, IO线程读取文件偏移超出实际长度，结束线程!!!-----------\n", partition);
//                 loop = false;
//                 end_[partition] = true;
//                 break;
//             }
//             GetValue(cache, offset);
//             //if(ret == 0) break;
//             cache += 16384;
//             offset += 16384;  
//         }
//         //printf("------------ load thread: %d, file partition: %d load finish!!!-----------\n", partition, cnt);
//         //printf("------------ load thread: %d, 2 readFinish[partition]: %d!!!-----------\n", partition, readFinish[partition]);

//         cnt += READ_THREAD_NUM;

//         cache = valueCache_ + partition * PARTITION_SIZE * 1024 * 1024; //本区数据已经全部加载，重新覆盖缓存区

//         offset -= len * (READ_THREAD_NUM + 1);  //文件偏移到下一个分区，+2

//         curPartition -= READ_THREAD_NUM;//下一个待读文件分区

//         loadFinish[partition] = true;
//         readFinish[partition] = false;
//         invertedOrderCnt_[partition] = 0;
//     }
//         readCondition_[partition].notify_one();//唤醒等待的测试线程
//     }        
// }

// void ValueFile::CreateReadLoadThread(int state) {
//     //创建DIO线程，预读，以及在后台加载文件到内存
//     //std::thread read_threads[READ_THREAD_NUM];
//     printf("CreateReadLoadThread\n");
//     if(state == 1) {
//         for(int i = 0; i < READ_THREAD_NUM; ++i) { 
//             //partitionInMemSet_[i] = i;
//             read_threads_[i] = std::thread(&ValueFile::LoadDataToMem, this, i);
//         }
//     } else {
//         for(int i = 0; i < READ_THREAD_NUM; ++i) { 
//             //int cur = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024)) * READ_THREAD_NUM + partition;   
//             //partitionInMemSet_[i] = i;
//             read_threads_[i] = std::thread(&ValueFile::LoadDataToMemRand, this, i);
//         }
//     }

//     isThreadsCreate = true;
// }

// void ValueFile::FlushValueToDisk() {
//     pwrite(valueFileFd_, valueCacheBuffer_, valueCacheBufferPosition_ * VALUE_SIZE, valueFilePosition_);
//     valueFilePosition_ += valueCacheBufferPosition_ * VALUE_SIZE;
//     valueCacheBufferPosition_ = 0;
//     *((size_t*)(this->valueMetaCacheBuffer_)) = valueCacheBufferPosition_;
//     *((size_t*)(this->valueMetaCacheBuffer_ + sizeof(size_t))) = valueFilePosition_;
//     memset(valueCacheBuffer_, 0, VALUE_BLOCK_SIZE);
// }

// void ValueFile::RecoverValue() {

// }


